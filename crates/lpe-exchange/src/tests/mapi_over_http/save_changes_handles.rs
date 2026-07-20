use super::*;

#[tokio::test]
async fn mapi_over_http_calendar_import_save_restores_containing_folder_response_handle() {
    // Outlook uploads a new appointment through ImportMessageChange, populates
    // the returned Message, then closes it with SaveChangesMessage. [MS-OXCMSG]
    // sections 2.2.3.3 and 3.2.5.3 require the response handle index to contain
    // the parent Folder even though the imported Message handle was closed.
    let imported_message_id = crate::mapi::identity::mapi_store_id(0x0df8_974b_7f66);
    let imported_source_key = crate::mapi::identity::source_key_for_object_id(imported_message_id);
    let imported_change_key = [
        0x67, 0x45, 0x48, 0x20, 0x69, 0x60, 0xca, 0x40, 0x9d, 0x80, 0x08, 0x17, 0x06, 0x0f, 0xa2,
        0xc1, 0x00, 0x00, 0x04, 0x57,
    ];
    let mut imported_predecessor_change_list = vec![imported_change_key.len() as u8];
    imported_predecessor_change_list.extend_from_slice(&imported_change_key);

    let store = FakeStore {
        session: Some(FakeStore::account()),
        calendar_collections: Arc::new(Mutex::new(vec![FakeStore::collection(
            "default", "calendar", "Calendar",
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

    let mut import_values = Vec::new();
    append_mapi_binary_property(&mut import_values, PID_TAG_SOURCE_KEY, &imported_source_key);
    append_mapi_i64_property(
        &mut import_values,
        PID_TAG_LAST_MODIFICATION_TIME,
        test_filetime("2026-07-20", "09:00"),
    );
    append_mapi_binary_property(&mut import_values, PID_TAG_CHANGE_KEY, &imported_change_key);
    append_mapi_binary_property(
        &mut import_values,
        PID_TAG_PREDECESSOR_CHANGE_LIST,
        &imported_predecessor_change_list,
    );

    let mut appointment_values = Vec::new();
    append_mapi_utf16_property(
        &mut appointment_values,
        PID_TAG_MESSAGE_CLASS_W,
        "IPM.Appointment",
    );
    append_mapi_utf16_property(
        &mut appointment_values,
        PID_TAG_SUBJECT_W,
        "Calendar response handle",
    );
    append_mapi_i64_property(
        &mut appointment_values,
        0x0060_0040,
        test_filetime("2026-07-20", "09:00"),
    );
    append_mapi_i64_property(
        &mut appointment_values,
        0x0061_0040,
        test_filetime("2026-07-20", "09:30"),
    );

    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, crate::mapi::identity::CALENDAR_FOLDER_ID);
    rops.extend_from_slice(&[
        0x7e, 0x00, 0x01, 0x02, 0x01, // RopSynchronizationOpenCollector, contents.
        0x72, 0x00, 0x02, 0x03, 0x01, // RopSynchronizationImportMessageChange.
    ]);
    rops.extend_from_slice(&4u16.to_le_bytes());
    rops.extend_from_slice(&import_values);
    append_rop_set_properties(&mut rops, 3, 4, &appointment_values);
    append_rop_save_changes_message_with_flags(&mut rops, 3, 3, 0x08);

    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = response_bytes(response).await;
    let (response_rops, response_handles) = response_rops_and_handles_from_execute_body(&body);
    assert!(
        contains_bytes(&response_rops, &[0x72, 0x03, 0, 0, 0, 0]),
        "ImportMessageChange failed: {response_rops:02x?}"
    );
    assert!(
        contains_bytes(&response_rops, &[0x0c, 0x03, 0, 0, 0, 0]),
        "SaveChangesMessage failed: {response_rops:02x?}"
    );
    assert_eq!(
        response_handles[3], response_handles[1],
        "SaveChangesMessage response handle must be the containing Calendar folder"
    );
}

#[tokio::test]
async fn mapi_over_http_failed_save_keeps_the_open_message_response_handle() {
    // [MS-OXCMSG] sections 2.2.3.3.1 and 3.2.5.3 define the containing
    // Folder response handle for a successful SaveChangesMessage. An error
    // response must not silently replace the still-open Message handle.
    const PID_TAG_SCHEDULE_INFO_APPOINTMENT_TOMBSTONE: u32 = 0x686A_0102;

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

    let local_freebusy_id = crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFE4);
    let mut rops = Vec::new();
    append_rop_open_folder(
        &mut rops,
        0,
        1,
        crate::mapi::identity::FREEBUSY_DATA_FOLDER_ID,
    );
    append_rop_open_message(
        &mut rops,
        1,
        2,
        crate::mapi::identity::FREEBUSY_DATA_FOLDER_ID,
        local_freebusy_id,
    );
    rops.extend_from_slice(&[0x2B, 0x00, 0x02, 0x03]); // RopOpenStream, create.
    rops.extend_from_slice(&PID_TAG_SCHEDULE_INFO_APPOINTMENT_TOMBSTONE.to_le_bytes());
    rops.push(0x02);
    rops.extend_from_slice(&[0x01, 0x00, 0x03]); // RopRelease untouched stream.
    append_rop_save_changes_message_with_flags(&mut rops, 2, 2, 0x0A);

    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = response_bytes(response).await;
    let (response_rops, response_handles) = response_rops_and_handles_from_execute_body(&body);
    assert!(
        contains_bytes(&response_rops, &[0x0C, 0x02, 0x57, 0x00, 0x07, 0x80]),
        "Save must fail for the incomplete tombstone: {response_rops:02x?}"
    );
    assert_ne!(
        response_handles[2], response_handles[1],
        "a failed Save must retain the Message handle instead of replacing it with its Folder"
    );
    assert_ne!(
        response_handles[2],
        u32::MAX,
        "a failed Save must not clear the still-open Message handle"
    );
}
