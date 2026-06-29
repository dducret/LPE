use super::*;

#[tokio::test]
async fn mapi_over_http_execute_returns_private_mailbox_logon() {
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

    let legacy_dn = b"/o=LPE/ou=Exchange Administrative Group/cn=Recipients/cn=alice\0";
    let mut logon_rop = vec![0xFE, 0x00, 0x00, 0x01];
    logon_rop.extend_from_slice(&0x0100_0004u32.to_le_bytes());
    logon_rop.extend_from_slice(&0u32.to_le_bytes());
    logon_rop.extend_from_slice(&(legacy_dn.len() as u16).to_le_bytes());
    logon_rop.extend_from_slice(legacy_dn);

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&logon_rop, &[]));
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
    let response_rop = &rop_buffer[2..2 + response_rop_size];

    assert_eq!(response_rop[0], 0xFE);
    assert_eq!(response_rop[1], 0x00);
    assert_eq!(
        u32::from_le_bytes(response_rop[2..6].try_into().unwrap()),
        0
    );
    assert_eq!(response_rop[6], 0x01);
    let response_flags_offset = 7 + PRIVATE_LOGON_SPECIAL_FOLDER_ID_COUNT * 8;
    assert_eq!(response_rop[response_flags_offset], 0x07);
    assert_eq!(
        response_rop_size,
        62 + PRIVATE_LOGON_SPECIAL_FOLDER_ID_COUNT * 8
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
async fn mapi_over_http_execute_accepts_rca_wrapped_private_mailbox_logon() {
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

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = rca_wrapped_private_logon_execute_body(
        "alice@example.test",
        "Client=MS Connectivity Analyzer",
    );
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    let body = response_bytes(response).await;
    let rop_buffer_size = u32::from_le_bytes(body[12..16].try_into().unwrap()) as usize;
    let rop_buffer = &body[16..16 + rop_buffer_size];
    assert_eq!(u16::from_le_bytes(rop_buffer[0..2].try_into().unwrap()), 0);
    assert_eq!(
        u16::from_le_bytes(rop_buffer[2..4].try_into().unwrap()),
        0x0004
    );
    let payload_size = u16::from_le_bytes(rop_buffer[4..6].try_into().unwrap()) as usize;
    assert_eq!(
        u16::from_le_bytes(rop_buffer[6..8].try_into().unwrap()) as usize,
        payload_size
    );
    let payload = &rop_buffer[8..8 + payload_size];
    let response_rop_size = u16::from_le_bytes(payload[0..2].try_into().unwrap()) as usize;
    let response_rop = &payload[2..response_rop_size];

    assert_eq!(response_rop[0], 0xFE);
    assert_eq!(response_rop[1], 0x00);
    assert_eq!(
        u32::from_le_bytes(response_rop[2..6].try_into().unwrap()),
        0
    );
    assert_eq!(response_rop[6] & 0x01, 0x01);
    let response_flags_offset = 7 + PRIVATE_LOGON_SPECIAL_FOLDER_ID_COUNT * 8;
    let mailbox_guid_offset = response_flags_offset + 1;
    let replid_offset = mailbox_guid_offset + 16;
    let replica_guid_offset = replid_offset + 2;
    assert_eq!(response_rop[response_flags_offset], 0x07);
    assert_eq!(
        &response_rop[mailbox_guid_offset..mailbox_guid_offset + 16],
        &FakeStore::account().account_id.to_bytes_le()
    );
    assert_eq!(
        &response_rop[replid_offset..replid_offset + 2],
        &1u16.to_le_bytes()
    );
    assert_eq!(
        &response_rop[replica_guid_offset..replica_guid_offset + 16],
        &mapi_mailstore::STORE_REPLICA_GUID
    );
    assert_eq!(response_rop_size, response_rop.len() + 2);
    assert_eq!(
        u32::from_le_bytes(
            payload[response_rop_size..response_rop_size + 4]
                .try_into()
                .unwrap()
        ),
        1
    );
}

#[tokio::test]
async fn mapi_over_http_execute_returns_logon_replid_guid_map_for_outlook_bootstrap() {
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

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let logon_request = hex_bytes(
        "0200000063000000000004005b005b005700fe0000010c0400210000000047002f6f3d4c50452f6f753d45786368616e67652041646d696e6973747261746976652047726f75702f636e3d526563697069656e74732f636e3d746573742d6c2d702d652d636800ffffffff0780000000000000",
    );
    let logon_response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &logon_request)
        .await
        .unwrap();
    assert_eq!(logon_response.status(), StatusCode::OK);
    assert_eq!(logon_response.headers().get("x-responsecode").unwrap(), "0");

    renew_mapi_request_id(&mut execute_headers);
    let replid_request = hex_bytes(
        "020000001b00000000000400130013000f0007000000000000010002013866010000000780000000000000",
    );
    let replid_response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &replid_request)
        .await
        .unwrap();

    assert_eq!(replid_response.status(), StatusCode::OK);
    assert_eq!(
        replid_response.headers().get("x-responsecode").unwrap(),
        "0"
    );
    let body = response_bytes(replid_response).await;
    let rop_buffer_size = u32::from_le_bytes(body[12..16].try_into().unwrap()) as usize;
    let rop_buffer = &body[16..16 + rop_buffer_size];
    assert_eq!(&rop_buffer[0..4], &[0, 0, 4, 0]);
    let payload_size = u16::from_le_bytes(rop_buffer[4..6].try_into().unwrap()) as usize;
    let payload = &rop_buffer[8..8 + payload_size];
    let response_rop_size = u16::from_le_bytes(payload[0..2].try_into().unwrap()) as usize;
    let response_rop = &payload[2..response_rop_size];

    assert_eq!(response_rop[0], 0x07);
    assert_eq!(response_rop[1], 0x00);
    assert_eq!(
        u32::from_le_bytes(response_rop[2..6].try_into().unwrap()),
        0
    );
    assert_eq!(response_rop[6], 0);
    assert_eq!(
        u16::from_le_bytes(response_rop[7..9].try_into().unwrap()),
        18
    );
    assert_eq!(&response_rop[9..11], &1u16.to_le_bytes());
    assert_eq!(&response_rop[11..27], &mapi_mailstore::STORE_REPLICA_GUID);
    assert_eq!(response_rop_size, response_rop.len() + 2);
    assert_eq!(&payload[response_rop_size..], &1u32.to_le_bytes());

    renew_mapi_request_id(&mut execute_headers);
    let named_property_request = hex_bytes(
        "020000003e00000000000400360036003200560000020200000820060000000000c00000000000004680850000000820060000000000c00000000000004681850000010000000780000000000000",
    );
    let named_property_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &named_property_request,
        )
        .await
        .unwrap();

    assert_eq!(named_property_response.status(), StatusCode::OK);
    let body = response_bytes(named_property_response).await;
    let rop_buffer_size = u32::from_le_bytes(body[12..16].try_into().unwrap()) as usize;
    let rop_buffer = &body[16..16 + rop_buffer_size];
    assert_eq!(&rop_buffer[0..4], &[0, 0, 4, 0]);
    let payload_size = u16::from_le_bytes(rop_buffer[4..6].try_into().unwrap()) as usize;
    let payload = &rop_buffer[8..8 + payload_size];
    let response_rop_size = u16::from_le_bytes(payload[0..2].try_into().unwrap()) as usize;
    let response_rop = &payload[2..response_rop_size];

    assert_eq!(response_rop[0], 0x56);
    assert_eq!(response_rop[1], 0x00);
    assert_eq!(
        u32::from_le_bytes(response_rop[2..6].try_into().unwrap()),
        0
    );
    assert_eq!(
        u16::from_le_bytes(response_rop[6..8].try_into().unwrap()),
        2
    );
    assert_eq!(&response_rop[8..10], &0x8003u16.to_le_bytes());
    assert_eq!(&response_rop[10..12], &0x8004u16.to_le_bytes());
    assert_eq!(response_rop_size, response_rop.len() + 2);
    assert_eq!(&payload[response_rop_size..], &1u32.to_le_bytes());

    renew_mapi_request_id(&mut execute_headers);
    let ipm_subtree_property_request = hex_bytes(
        "020000002c00000000000400240024001c00020000010100000000000004000700010000000001000201047c01000000ffffffff0780000000000000",
    );
    let ipm_subtree_property_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &ipm_subtree_property_request,
        )
        .await
        .unwrap();

    assert_eq!(ipm_subtree_property_response.status(), StatusCode::OK);
    let body = response_bytes(ipm_subtree_property_response).await;
    let rop_buffer_size = u32::from_le_bytes(body[12..16].try_into().unwrap()) as usize;
    let rop_buffer = &body[16..16 + rop_buffer_size];
    assert_eq!(&rop_buffer[0..4], &[0, 0, 4, 0]);
    let payload_size = u16::from_le_bytes(rop_buffer[4..6].try_into().unwrap()) as usize;
    let payload = &rop_buffer[8..8 + payload_size];
    let response_rop_size = u16::from_le_bytes(payload[0..2].try_into().unwrap()) as usize;
    let response_rops = &payload[2..response_rop_size];

    assert_eq!(response_rops[0], 0x02);
    assert_eq!(response_rops[1], 0x01);
    assert_eq!(
        u32::from_le_bytes(response_rops[2..6].try_into().unwrap()),
        0
    );
    assert_eq!(response_rops[8], 0x07);
    assert_eq!(response_rops[9], 0x01);
    assert_eq!(
        u32::from_le_bytes(response_rops[10..14].try_into().unwrap()),
        0
    );
    assert_eq!(response_rop_size, response_rops.len() + 2);
    assert_eq!(
        &payload[response_rop_size..response_rop_size + 8],
        &[1, 0, 0, 0, 2, 0, 0, 0]
    );

    renew_mapi_request_id(&mut execute_headers);
    let folder_set_properties_request = hex_bytes(
        "020000002f000000000004002700270023000a00001c0001000201047c14003bccd33e05e40d41a4e87c7d9d249ff501000000020000000780000000000000",
    );
    let folder_set_properties_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &folder_set_properties_request,
        )
        .await
        .unwrap();

    assert_eq!(folder_set_properties_response.status(), StatusCode::OK);
    let body = response_bytes(folder_set_properties_response).await;
    let rop_buffer_size = u32::from_le_bytes(body[12..16].try_into().unwrap()) as usize;
    let rop_buffer = &body[16..16 + rop_buffer_size];
    assert_eq!(&rop_buffer[0..4], &[0, 0, 4, 0]);
    let payload_size = u16::from_le_bytes(rop_buffer[4..6].try_into().unwrap()) as usize;
    let payload = &rop_buffer[8..8 + payload_size];
    let response_rop_size = u16::from_le_bytes(payload[0..2].try_into().unwrap()) as usize;
    let response_rop = &payload[2..response_rop_size];

    assert_eq!(response_rop[0], 0x0A);
    assert_eq!(response_rop[1], 0x00);
    assert_eq!(
        u32::from_le_bytes(response_rop[2..6].try_into().unwrap()),
        0
    );
    assert_eq!(
        u16::from_le_bytes(response_rop[6..8].try_into().unwrap()),
        0
    );
    assert_eq!(response_rop_size, response_rop.len() + 2);
    assert_eq!(&payload[response_rop_size..], &2u32.to_le_bytes());

    renew_mapi_request_id(&mut execute_headers);
    let release_and_local_replica_ids_request = hex_bytes(
        "020000001c00000000000400140014000c000100007f00010000010002000000010000000780000000000000",
    );
    let release_and_local_replica_ids_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &release_and_local_replica_ids_request,
        )
        .await
        .unwrap();

    assert_eq!(
        release_and_local_replica_ids_response.status(),
        StatusCode::OK
    );
    let body = response_bytes(release_and_local_replica_ids_response).await;
    let rop_buffer_size = u32::from_le_bytes(body[12..16].try_into().unwrap()) as usize;
    let rop_buffer = &body[16..16 + rop_buffer_size];
    assert_eq!(&rop_buffer[0..4], &[0, 0, 4, 0]);
    let payload_size = u16::from_le_bytes(rop_buffer[4..6].try_into().unwrap()) as usize;
    let payload = &rop_buffer[8..8 + payload_size];
    let response_rop_size = u16::from_le_bytes(payload[0..2].try_into().unwrap()) as usize;
    let response_rop = &payload[2..response_rop_size];

    assert_eq!(response_rop[0], 0x7F);
    assert_eq!(response_rop[1], 0x01);
    assert_eq!(
        u32::from_le_bytes(response_rop[2..6].try_into().unwrap()),
        0
    );
    assert_eq!(&response_rop[6..22], &mapi_mailstore::STORE_REPLICA_GUID);
    assert_eq!(response_rop.len(), 28);
    assert!(response_rop[22..28].iter().any(|byte| *byte != 0));
    assert_eq!(response_rop_size, response_rop.len() + 2);
    assert_eq!(
        &payload[response_rop_size..response_rop_size + 8],
        &[0xff, 0xff, 0xff, 0xff, 1, 0, 0, 0]
    );

    renew_mapi_request_id(&mut execute_headers);
    let release_and_receive_folder_request = hex_bytes(
        "0200000019000000000004001100110009000100002700010003000000010000000780000000000000",
    );
    let release_and_receive_folder_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &release_and_receive_folder_request,
        )
        .await
        .unwrap();

    assert_eq!(release_and_receive_folder_response.status(), StatusCode::OK);
    let body = response_bytes(release_and_receive_folder_response).await;
    let rop_buffer_size = u32::from_le_bytes(body[12..16].try_into().unwrap()) as usize;
    let rop_buffer = &body[16..16 + rop_buffer_size];
    assert_eq!(&rop_buffer[0..4], &[0, 0, 4, 0]);
    let payload_size = u16::from_le_bytes(rop_buffer[4..6].try_into().unwrap()) as usize;
    let payload = &rop_buffer[8..8 + payload_size];
    let response_rop_size = u16::from_le_bytes(payload[0..2].try_into().unwrap()) as usize;
    let response_rop = &payload[2..response_rop_size];

    assert_eq!(response_rop[0], 0x27);
    assert_eq!(response_rop[1], 0x01);
    assert_eq!(
        u32::from_le_bytes(response_rop[2..6].try_into().unwrap()),
        0
    );
    assert_eq!(
        crate::mapi::identity::object_id_from_wire_id(&response_rop[6..14]).unwrap(),
        test_mapi_folder_id(5)
    );
    assert_eq!(&response_rop[14..], b"\0");
    assert_eq!(response_rop_size, response_rop.len() + 2);
    assert_eq!(
        &payload[response_rop_size..response_rop_size + 8],
        &[0xff, 0xff, 0xff, 0xff, 1, 0, 0, 0]
    );

    renew_mapi_request_id(&mut execute_headers);
    let mut hierarchy_sync_rops = Vec::new();
    append_rop_open_folder(&mut hierarchy_sync_rops, 0, 1, test_mapi_folder_id(1));
    hierarchy_sync_rops.extend_from_slice(&[
        0x70, 0x00, 0x01, 0x02, // RopSynchronizationConfigure
        0x02, 0x09, 0x01, 0x01, // hierarchy sync, Unicode send/options
        0x00, 0x00, // RestrictionDataSize
        0x00, 0x00, 0x00, 0x00, // SynchronizationExtraFlags
    ]);
    let sync_property_tags = [
        0x3601_0003u32,
        0x3602_0003,
        0x3603_0003,
        0x0E08_0003,
        0x0FF4_0003,
        0x3FE0_0102,
        0x3FE1_0102,
        0x0E27_0102,
    ];
    hierarchy_sync_rops.extend_from_slice(&(sync_property_tags.len() as u16).to_le_bytes());
    for tag in sync_property_tags {
        hierarchy_sync_rops.extend_from_slice(&tag.to_le_bytes());
    }
    hierarchy_sync_rops.extend_from_slice(&[
        0x75, 0x00, 0x02, // RopSynchronizationUploadStateStreamBegin
    ]);
    hierarchy_sync_rops.extend_from_slice(&0x4017_0003u32.to_le_bytes());
    hierarchy_sync_rops.extend_from_slice(&0u32.to_le_bytes());
    hierarchy_sync_rops.extend_from_slice(&[
        0x77, 0x00, 0x02, // RopSynchronizationUploadStateStreamEnd
        0x75, 0x00, 0x02, // RopSynchronizationUploadStateStreamBegin
    ]);
    hierarchy_sync_rops.extend_from_slice(&0x6796_0102u32.to_le_bytes());
    hierarchy_sync_rops.extend_from_slice(&0u32.to_le_bytes());
    hierarchy_sync_rops.extend_from_slice(&[
        0x77, 0x00, 0x02, // RopSynchronizationUploadStateStreamEnd
    ]);
    hierarchy_sync_rops.extend_from_slice(&[
        0x4E, 0x00, 0x02, // RopFastTransferSourceGetBuffer
    ]);
    hierarchy_sync_rops.extend_from_slice(&0xBABEu16.to_le_bytes());
    hierarchy_sync_rops.extend_from_slice(&0x7BC0u16.to_le_bytes());
    let hierarchy_sync_configure_request = execute_body(&crate::tests::rop_buffer(
        &hierarchy_sync_rops,
        &[1, u32::MAX, u32::MAX],
    ));
    let hierarchy_sync_configure_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &hierarchy_sync_configure_request,
        )
        .await
        .unwrap();

    assert_eq!(hierarchy_sync_configure_response.status(), StatusCode::OK);
    let response_rops =
        response_rops_from_execute_response(hierarchy_sync_configure_response).await;

    assert!(contains_bytes(
        &response_rops,
        &[0x70, 0x02, 0x00, 0x00, 0x00, 0x00]
    ));
    assert!(contains_bytes(
        &response_rops,
        &[0x70, 0x02, 0x00, 0x00, 0x00, 0x00, 0x75, 0x02, 0x00, 0x00, 0x00, 0x00,]
    ));
    assert!(!contains_bytes(
        &response_rops,
        &[0x70, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x75,]
    ));
    assert!(contains_bytes(
        &response_rops,
        &[0x75, 0x02, 0x00, 0x00, 0x00, 0x00]
    ));
    assert!(contains_bytes(
        &response_rops,
        &[0x77, 0x02, 0x00, 0x00, 0x00, 0x00]
    ));
    assert!(contains_bytes(
        &response_rops,
        &[0x4E, 0x02, 0x00, 0x00, 0x00, 0x00]
    ));
    assert!(!contains_bytes(&response_rops, &[0x02, 0x01, 0x04, 0x80]));
    assert!(contains_bytes(
        &response_rops,
        &0x403A_0003u32.to_le_bytes()
    ));
    assert!(contains_bytes(
        &response_rops,
        &META_TAG_IDSET_GIVEN.to_le_bytes()
    ));
    assert!(contains_bytes(
        &response_rops,
        &0x6796_0102u32.to_le_bytes()
    ));
    assert!(contains_bytes(
        &response_rops,
        &0x403B_0003u32.to_le_bytes()
    ));
    assert!(contains_bytes(
        &response_rops,
        &0x4014_0003u32.to_le_bytes()
    ));
    assert!(!contains_bytes(&response_rops, b"LPE-MAPI-SYNC\0"));

    renew_mapi_request_id(&mut execute_headers);
    let address_types_request =
        hex_bytes("020000001100000000000400090009000500490000010000000780000000000000");
    let address_types_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &address_types_request,
        )
        .await
        .unwrap();

    assert_eq!(address_types_response.status(), StatusCode::OK);
    let body = response_bytes(address_types_response).await;
    let rop_buffer_size = u32::from_le_bytes(body[12..16].try_into().unwrap()) as usize;
    let rop_buffer = &body[16..16 + rop_buffer_size];
    assert_eq!(&rop_buffer[0..4], &[0, 0, 4, 0]);
    let payload_size = u16::from_le_bytes(rop_buffer[4..6].try_into().unwrap()) as usize;
    let payload = &rop_buffer[8..8 + payload_size];
    let response_rop_size = u16::from_le_bytes(payload[0..2].try_into().unwrap()) as usize;
    let response_rop = &payload[2..response_rop_size];

    assert_eq!(response_rop[0], 0x49);
    assert_eq!(response_rop[1], 0x00);
    assert_eq!(
        u32::from_le_bytes(response_rop[2..6].try_into().unwrap()),
        0
    );
    assert_eq!(
        u16::from_le_bytes(response_rop[6..8].try_into().unwrap()),
        2
    );
    assert_eq!(
        u16::from_le_bytes(response_rop[8..10].try_into().unwrap()) as usize,
        b"EX\0SMTP\0".len()
    );
    assert_eq!(&response_rop[10..], b"EX\0SMTP\0");
    assert_eq!(&payload[response_rop_size..], &1u32.to_le_bytes());
}

#[tokio::test]
async fn mapi_over_http_logon_advertises_openable_additional_ren_entryids_ex() {
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

    let mut rops = vec![
        0xFE, 0x00, 0x00, 0x01, // RopLogon
    ];
    rops.extend_from_slice(&0u32.to_le_bytes());
    rops.extend_from_slice(&0u32.to_le_bytes());
    rops.extend_from_slice(&0u16.to_le_bytes());
    append_rop_get_properties_specific(&mut rops, 1, &[PID_TAG_ADDITIONAL_REN_ENTRY_IDS_EX]);

    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    let get_props_offset = response_rops
        .windows(7)
        .rposition(|window| window == [0x07, 0x01, 0, 0, 0, 0, 0])
        .expect("AdditionalRenEntryIdsEx GetProperties response");
    let value_offset = get_props_offset + 7;
    let value_len = u16::from_le_bytes(
        response_rops[value_offset..value_offset + 2]
            .try_into()
            .unwrap(),
    ) as usize;
    let value = &response_rops[value_offset + 2..value_offset + 2 + value_len];
    let entries = additional_ren_entry_ids_ex_entries(value);
    let expected = vec![
        (
            0x8001,
            crate::mapi::identity::RSS_FEEDS_FOLDER_ID,
            "RSS Feeds",
            "IPF.Note.OutlookHomepage",
        ),
        (
            0x8002,
            crate::mapi::identity::TRACKED_MAIL_PROCESSING_FOLDER_ID,
            "Tracked Mail Processing",
            "IPF.Note",
        ),
        (
            0x8004,
            crate::mapi::identity::TODO_SEARCH_FOLDER_ID,
            "To-Do",
            "IPF.Task",
        ),
        (
            0x8006,
            crate::mapi::identity::CONVERSATION_ACTION_SETTINGS_FOLDER_ID,
            "Conversation Action Settings",
            "IPF.Configuration",
        ),
        (
            0x8007,
            crate::mapi::identity::QUICK_STEP_SETTINGS_FOLDER_ID,
            "Quick Step Settings",
            "IPF.Configuration",
        ),
        (
            0x8008,
            crate::mapi::identity::SUGGESTED_CONTACTS_FOLDER_ID,
            "Suggested Contacts",
            "IPF.Contact",
        ),
        (
            0x8009,
            crate::mapi::identity::CONTACTS_SEARCH_FOLDER_ID,
            "Contacts Search",
            "IPF.Contact",
        ),
        (
            0x800A,
            crate::mapi::identity::IM_CONTACT_LIST_FOLDER_ID,
            "IM Contact List",
            "IPF.Contact.MOC.ImContactList",
        ),
        (
            0x800B,
            crate::mapi::identity::QUICK_CONTACTS_FOLDER_ID,
            "Quick Contacts",
            "IPF.Contact.MOC.QuickContacts",
        ),
        (
            0x800F,
            crate::mapi::identity::ARCHIVE_FOLDER_ID,
            "Archive",
            "IPF.Note",
        ),
    ];
    assert_eq!(
        entries,
        expected
            .iter()
            .map(|(persist_id, folder_id, _, _)| (*persist_id, *folder_id))
            .collect::<Vec<_>>()
    );

    for (_, folder_id, display_name, container_class) in expected {
        renew_mapi_request_id(&mut execute_headers);
        let mut rops = Vec::new();
        append_rop_open_folder(&mut rops, 0, 1, folder_id);
        append_rop_get_properties_specific(
            &mut rops,
            1,
            &[PID_TAG_DISPLAY_NAME_W, PID_TAG_CONTAINER_CLASS_W],
        );
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
        assert_eq!(response_rops[0], 0x02);
        assert_eq!(
            u32::from_le_bytes(response_rops[2..6].try_into().unwrap()),
            0
        );
        assert!(contains_bytes(&response_rops, &utf16z(display_name)));
        assert!(contains_bytes(&response_rops, &utf16z(container_class)));
    }
}

#[tokio::test]
async fn mapi_over_http_execute_handles_mailbox_store_bootstrap_rops() {
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
    append_mapi_wire_id(&mut rops, test_mapi_folder_id(5));
    rops.push(0);
    rops.extend_from_slice(&[
        0x09, 0x00, 0x01, // RopGetPropertiesList
        0x05, 0x00, 0x01, 0x02, 0x00, // RopGetContentsTable
        0x16, 0x00, 0x02, // RopGetStatus
        0x17, 0x00, 0x02, // RopQueryPosition
        0x81, 0x00, 0x02, // RopResetTable
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
    let rop_buffer = &body[16..16 + rop_buffer_size];
    let response_rop_size = u16::from_le_bytes(rop_buffer[0..2].try_into().unwrap()) as usize;
    let response_rops = &rop_buffer[2..2 + response_rop_size];

    let props_list_offset = 8;
    assert_eq!(response_rops[props_list_offset], 0x09);
    let folder_property_count = u16::from_le_bytes(
        response_rops[props_list_offset + 6..props_list_offset + 8]
            .try_into()
            .unwrap(),
    ) as usize;
    assert!(contains_bytes(response_rops, &0x6748_0014u32.to_le_bytes()));
    assert!(contains_bytes(response_rops, &0x3601_0003u32.to_le_bytes()));
    assert!(contains_bytes(response_rops, &0x0FF4_0003u32.to_le_bytes()));
    assert!(contains_bytes(response_rops, &0x3613_001Fu32.to_le_bytes()));

    let contents_offset = props_list_offset + 8 + folder_property_count * 4;
    assert_eq!(response_rops[contents_offset], 0x05);
    assert_eq!(response_rops[contents_offset + 1], 0x02);
    assert_eq!(
        u32::from_le_bytes(
            response_rops[contents_offset + 2..contents_offset + 6]
                .try_into()
                .unwrap()
        ),
        0
    );
    assert_eq!(
        u32::from_le_bytes(
            response_rops[contents_offset + 6..contents_offset + 10]
                .try_into()
                .unwrap()
        ),
        0
    );

    let status_offset = contents_offset + 10;
    assert_eq!(
        &response_rops[status_offset..status_offset + 7],
        &[0x16, 0x02, 0, 0, 0, 0, 0]
    );
    let position_offset = status_offset + 7;
    assert_eq!(response_rops[position_offset], 0x17);
    assert_eq!(
        u32::from_le_bytes(
            response_rops[position_offset + 2..position_offset + 6]
                .try_into()
                .unwrap()
        ),
        0
    );
    let reset_offset = position_offset + 14;
    assert_eq!(
        &response_rops[reset_offset..reset_offset + 6],
        &[0x81, 0x02, 0, 0, 0, 0]
    );
    let query_offset = reset_offset + 6;
    assert_eq!(
        &response_rops[query_offset..],
        &[0x15, 0x02, 0xB9, 0x04, 0, 0]
    );
}

#[tokio::test]
async fn mapi_over_http_execute_returns_receive_folder_and_store_state() {
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

    let mut rops = vec![0x27, 0x00, 0x00];
    rops.extend_from_slice(b"IPM.Note\0");
    rops.extend_from_slice(&[
        0x68, 0x00, 0x00, // RopGetReceiveFolderTable
        0x7B, 0x00, 0x00, // RopGetStoreState
    ]);

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&rops, &[1]));
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

    assert_eq!(response_rops[0], 0x27);
    assert_eq!(
        crate::mapi::identity::object_id_from_wire_id(&response_rops[6..14]).unwrap(),
        test_mapi_folder_id(5)
    );
    assert!(contains_bytes(response_rops, b"IPM.Note\0"));

    let table_offset = 23;
    assert_eq!(response_rops[table_offset], 0x68);
    assert_eq!(
        u32::from_le_bytes(
            response_rops[table_offset + 6..table_offset + 10]
                .try_into()
                .unwrap()
        ),
        4
    );
    assert!(contains_bytes(response_rops, b"IPM\0"));
    assert!(contains_bytes(response_rops, b"IPM.Note\0"));
    assert!(contains_bytes(response_rops, b"IPM.Appointment\0"));
    let mut row_offset = table_offset + 10;
    let mut receive_folder_rows = Vec::new();
    for _ in 0..4 {
        assert_eq!(response_rops[row_offset], 0);
        row_offset += 1;
        let folder_id = crate::mapi::identity::object_id_from_wire_id(
            &response_rops[row_offset..row_offset + 8],
        )
        .unwrap();
        row_offset += 8;
        let string_start = row_offset;
        while response_rops[row_offset] != 0 {
            row_offset += 1;
        }
        let message_class = std::str::from_utf8(&response_rops[string_start..row_offset])
            .unwrap()
            .to_string();
        row_offset += 1;
        let last_modified = u64::from_le_bytes(
            response_rops[row_offset..row_offset + 8]
                .try_into()
                .unwrap(),
        );
        row_offset += 8;
        receive_folder_rows.push((message_class, folder_id, last_modified));
    }
    assert!(receive_folder_rows
        .iter()
        .any(|(message_class, folder_id, last_modified)| {
            message_class.is_empty()
                && *folder_id == crate::mapi::identity::INBOX_FOLDER_ID
                && *last_modified
                    == mapi_mailstore::filetime_from_change_number(
                        mapi_mailstore::change_number_for_store_id(
                            crate::mapi::identity::INBOX_FOLDER_ID,
                        ),
                    )
        }));
    assert!(receive_folder_rows
        .iter()
        .any(|(message_class, folder_id, _)| {
            message_class == "IPM.Appointment"
                && *folder_id == crate::mapi::identity::CALENDAR_FOLDER_ID
        }));
    assert!(receive_folder_rows
        .iter()
        .any(|(message_class, folder_id, last_modified)| {
            message_class == "IPM.Appointment"
                && *folder_id == crate::mapi::identity::CALENDAR_FOLDER_ID
                && *last_modified
                    == mapi_mailstore::filetime_from_change_number(
                        mapi_mailstore::change_number_for_store_id(
                            crate::mapi::identity::CALENDAR_FOLDER_ID,
                        ),
                    )
        }));

    let store_offset = response_rops.len() - 10;
    assert_eq!(response_rops[store_offset], 0x7B);
    assert_eq!(
        u32::from_le_bytes(
            response_rops[store_offset + 6..store_offset + 10]
                .try_into()
                .unwrap()
        ),
        0
    );
}

#[tokio::test]
async fn mapi_over_http_get_receive_folder_requires_private_logon_handle() {
    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, crate::mapi::identity::INBOX_FOLDER_ID);
    rops.extend_from_slice(&[0x27, 0x00, 0x01]); // RopGetReceiveFolder on folder handle.
    rops.extend_from_slice(b"IPM.Appointment\0");

    let response_rops = execute_rops_response_rops(&rops, &[1, u32::MAX]).await;

    assert!(contains_bytes(
        &response_rops,
        &[0x27, 0x01, 0x02, 0x01, 0x04, 0x80]
    ));
}

#[tokio::test]
async fn mapi_over_http_set_receive_folder_requires_private_logon_handle() {
    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, crate::mapi::identity::INBOX_FOLDER_ID);
    rops.extend_from_slice(&[0x26, 0x00, 0x01]); // RopSetReceiveFolder on folder handle.
    append_mapi_wire_id(&mut rops, crate::mapi::identity::CALENDAR_FOLDER_ID);
    rops.extend_from_slice(b"IPM.Appointment\0");

    let response_rops = execute_rops_response_rops(&rops, &[1, u32::MAX]).await;

    assert!(contains_bytes(
        &response_rops,
        &[0x26, 0x01, 0x02, 0x01, 0x04, 0x80]
    ));
}

#[tokio::test]
async fn mapi_over_http_get_receive_folder_uses_message_class_matching() {
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

    let mut rops = vec![0x27, 0x00, 0x00];
    rops.extend_from_slice(b"IPM.Note.Custom\0");
    rops.extend_from_slice(&[0x27, 0x00, 0x00]);
    rops.extend_from_slice(b"IPM.Appointment\0");
    rops.extend_from_slice(&[0x27, 0x00, 0x00]);
    rops.extend_from_slice(b"IPM.Appointment.Custom\0");
    rops.extend_from_slice(&[0x27, 0x00, 0x00]);
    rops.extend_from_slice(b"IPM.AppointmentCustom\0");
    rops.extend_from_slice(&[0x27, 0x00, 0x00]);
    rops.extend_from_slice(b"IPM.MY.Class\0");
    rops.extend_from_slice(&[0x27, 0x00, 0x00]);
    rops.extend_from_slice(b"MY.Class\0");
    rops.extend_from_slice(&[0x27, 0x00, 0x00]);
    rops.extend_from_slice(b".Invalid\0");

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&rops, &[1]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(&response_rops, b"IPM.Note\0"));
    let mut calendar_response = vec![0x27, 0x00, 0, 0, 0, 0];
    append_mapi_wire_id(&mut calendar_response, test_mapi_folder_id(16));
    calendar_response.extend_from_slice(b"IPM.Appointment\0");
    assert!(contains_bytes(&response_rops, calendar_response.as_slice()));
    assert_eq!(
        response_rops
            .windows(calendar_response.len())
            .filter(|window| *window == calendar_response.as_slice())
            .count(),
        2
    );
    let mut ipm_response = vec![0x27, 0x00, 0, 0, 0, 0];
    append_mapi_wire_id(&mut ipm_response, test_mapi_folder_id(5));
    ipm_response.extend_from_slice(b"IPM\0");
    assert!(contains_bytes(&response_rops, ipm_response.as_slice()));
    let mut unmatched_response = vec![0x27, 0x00, 0, 0, 0, 0];
    append_mapi_wire_id(&mut unmatched_response, test_mapi_folder_id(5));
    unmatched_response.push(0);
    assert!(contains_bytes(
        &response_rops,
        unmatched_response.as_slice()
    ));
    assert!(contains_bytes(
        &response_rops,
        &[0x27, 0x00, 0x57, 0x00, 0x07, 0x80]
    ));
}

#[tokio::test]
async fn mapi_over_http_get_receive_folder_empty_class_returns_empty_explicit_message_class() {
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

    let rops = vec![0x27, 0x00, 0x00, 0x00];
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&rops, &[1]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    assert_eq!(response_rops[0], 0x27);
    assert_eq!(
        crate::mapi::identity::object_id_from_wire_id(&response_rops[6..14]).unwrap(),
        test_mapi_folder_id(5)
    );
    assert_eq!(&response_rops[14..], b"\0");
}

#[tokio::test]
async fn mapi_over_http_microsoft_get_store_state_accepts_live_handle_without_batch_drift() {
    let mut rops = vec![
        0x7B, 0x00, 0x01, // RopGetStoreState on missing handle 1.
    ];
    append_rop_open_folder(&mut rops, 0, 2, test_mapi_folder_id(5));
    rops.extend_from_slice(&[
        0x7B, 0x00, 0x02, // RopGetStoreState on live folder handle 2.
        0x7B, 0x00, 0x00, // RopGetStoreState succeeds on the logon handle.
    ]);

    let response_rops = execute_rops_response_rops(&rops, &[1, u32::MAX, u32::MAX]).await;

    assert!(contains_bytes(
        &response_rops,
        &[0x7B, 0x01, 0x02, 0x01, 0x04, 0x80]
    ));
    assert!(contains_bytes(&response_rops, &[0x02, 0x02, 0, 0, 0, 0]));
    assert!(contains_bytes(
        &response_rops,
        &[0x7B, 0x02, 0, 0, 0, 0, 0, 0, 0, 0]
    ));
    assert!(contains_bytes(
        &response_rops,
        &[0x7B, 0x00, 0, 0, 0, 0, 0, 0, 0, 0]
    ));
}

#[tokio::test]
async fn mapi_over_http_get_receive_folder_preserves_ipm_note_inbox_mapping() {
    let mut rops = vec![0x27, 0x00, 0x00];
    rops.extend_from_slice(b"IPM.Note\0");

    let response_rops = execute_rops_response_rops(&rops, &[1]).await;

    assert_eq!(&response_rops[..6], &[0x27, 0x00, 0, 0, 0, 0]);
    assert_eq!(
        &response_rops[6..14],
        &mapi_wire_id_bytes(crate::mapi::identity::INBOX_FOLDER_ID)
    );
    assert_eq!(&response_rops[14..], b"IPM.Note\0");
}

#[tokio::test]
async fn mapi_over_http_execute_returns_empty_transport_options_data() {
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

    let legacy_dn = b"/o=LPE/ou=Exchange Administrative Group/cn=Recipients/cn=alice\0";
    let mut rops = vec![0xFE, 0x00, 0x00, 0x01];
    rops.extend_from_slice(&0x0100_0004u32.to_le_bytes());
    rops.extend_from_slice(&0u32.to_le_bytes());
    rops.extend_from_slice(&(legacy_dn.len() as u16).to_le_bytes());
    rops.extend_from_slice(legacy_dn);
    rops.extend_from_slice(&[
        0x49, 0x00, 0x00, // RopGetAddressTypes
        0x6F, 0x00, 0x00, // RopOptionsData
    ]);
    rops.extend_from_slice(b"SMTP\0");
    rops.push(0);

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&rops, &[u32::MAX, u32::MAX]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(&response_rops, b"EX\0SMTP\0"));
    assert_eq!(
        &response_rops[response_rops.len() - 11..],
        &[0x6F, 0x00, 0, 0, 0, 0, 1, 0, 0, 0, 0]
    );
}
