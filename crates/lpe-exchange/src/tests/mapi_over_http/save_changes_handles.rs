use super::*;

fn last_get_properties_binary_value(response_rops: &[u8], handle_index: u8) -> Vec<u8> {
    let prefix = [0x07, handle_index, 0, 0, 0, 0];
    let offset = response_rops
        .windows(prefix.len())
        .rposition(|window| window == prefix)
        .expect("RopGetPropertiesSpecific success response");
    let flagged = response_rops[offset + prefix.len()] != 0;
    let mut value_offset = offset + prefix.len() + 1;
    if flagged {
        assert_eq!(
            response_rops[value_offset], 0,
            "the first flagged property must contain a value"
        );
        value_offset += 1;
    }
    let value_size = u16::from_le_bytes(
        response_rops[value_offset..value_offset + 2]
            .try_into()
            .unwrap(),
    ) as usize;
    value_offset += 2;
    response_rops[value_offset..value_offset + value_size].to_vec()
}

async fn pending_calendar_message_for_save_handle_test(
) -> (ExchangeService<FakeStore>, HeaderMap, u32, u32, u32) {
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

    let mut rops = mapi_private_logon_rops("alice");
    append_rop_open_folder(&mut rops, 0, 1, crate::mapi::identity::CALENDAR_FOLDER_ID);
    append_rop_create_message(&mut rops, 1, 2, crate::mapi::identity::CALENDAR_FOLDER_ID);
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[u32::MAX, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = response_bytes(response).await;
    let (response_rops, response_handles) = response_rops_and_handles_from_execute_body(&body);
    assert!(contains_bytes(&response_rops, &[0xFE, 0x00, 0, 0, 0, 0]));
    assert!(contains_bytes(&response_rops, &[0x02, 0x01, 0, 0, 0, 0]));
    assert!(contains_bytes(&response_rops, &[0x06, 0x02, 0, 0, 0, 0]));

    (
        service,
        execute_headers,
        response_handles[0],
        response_handles[1],
        response_handles[2],
    )
}

#[tokio::test]
async fn mapi_over_http_save_restores_released_parent_for_distinct_and_aliased_slots() {
    for response_handle_index in [1u8, 2u8] {
        let (service, mut execute_headers, logon_handle, parent_handle, message_handle) =
            pending_calendar_message_for_save_handle_test().await;
        let mut appointment_values = Vec::new();
        append_mapi_utf16_property(
            &mut appointment_values,
            PID_TAG_MESSAGE_CLASS_W,
            "IPM.Appointment",
        );
        append_mapi_utf16_property(
            &mut appointment_values,
            PID_TAG_SUBJECT_W,
            "Released parent response handle",
        );
        append_mapi_i64_property(
            &mut appointment_values,
            0x0060_0040,
            test_filetime("2026-08-12", "10:30"),
        );
        append_mapi_i64_property(
            &mut appointment_values,
            0x0061_0040,
            test_filetime("2026-08-12", "11:00"),
        );

        let mut save_rops = vec![0x01, 0x00, 0x01]; // Release the containing Folder.
        append_rop_set_properties(&mut save_rops, 2, 4, &appointment_values);
        append_rop_save_changes_message_with_flags(&mut save_rops, response_handle_index, 2, 0x08);
        append_rop_get_properties_specific(&mut save_rops, 2, &[PID_TAG_CHANGE_KEY]);
        renew_mapi_request_id(&mut execute_headers);
        let response = service
            .handle_mapi(
                MapiEndpoint::Emsmdb,
                &execute_headers,
                &execute_body(&rop_buffer(
                    &save_rops,
                    &[logon_handle, parent_handle, message_handle],
                )),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = response_bytes(response).await;
        let (response_rops, response_handles) = response_rops_and_handles_from_execute_body(&body);
        assert!(
            contains_bytes(
                &response_rops,
                &[0x0C, response_handle_index, 0, 0, 0, 0]
            ),
            "SaveChangesMessage failed for response index {response_handle_index}: {response_rops:02x?}"
        );
        assert!(
            mapi_get_properties_specific_standard_row_offset(&response_rops, 2).is_ok(),
            "saved Event was not readable later in the same Execute for response index {response_handle_index}: {response_rops:02x?}"
        );
        let restored_parent_handle = response_handles[usize::from(response_handle_index)];
        assert_ne!(restored_parent_handle, parent_handle);
        assert_ne!(restored_parent_handle, message_handle);
        assert_ne!(restored_parent_handle, u32::MAX);

        let mut verify_rops = Vec::new();
        append_rop_get_properties_specific(&mut verify_rops, 0, &[PID_TAG_DISPLAY_NAME_W]);
        append_rop_get_properties_specific(&mut verify_rops, 1, &[PID_TAG_DISPLAY_NAME_W]);
        append_rop_get_properties_specific(&mut verify_rops, 2, &[PID_TAG_SUBJECT_W]);
        renew_mapi_request_id(&mut execute_headers);
        let response = service
            .handle_mapi(
                MapiEndpoint::Emsmdb,
                &execute_headers,
                &execute_body(&rop_buffer(
                    &verify_rops,
                    &[restored_parent_handle, parent_handle, message_handle],
                )),
            )
            .await
            .unwrap();
        let verify_response = response_rops_from_execute_response(response).await;
        assert!(
            contains_bytes(&verify_response, &[0x07, 0x00, 0, 0, 0, 0]),
            "restored response handle is not a readable Folder: {verify_response:02x?}"
        );
        assert!(contains_bytes(
            &verify_response,
            &[0x07, 0x01, 0x08, 0x01, 0x04, 0x80]
        ));
        assert!(contains_bytes(
            &verify_response,
            &[0x07, 0x02, 0x08, 0x01, 0x04, 0x80]
        ));
    }
}

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
    let root_change_key = mapi_mailstore::change_key_for_change_number(
        mapi_mailstore::change_number_for_store_id(crate::mapi::identity::ROOT_FOLDER_ID),
    );
    assert_ne!(
        imported_change_key.as_slice(),
        root_change_key.as_slice(),
        "the regression fixture must distinguish the imported Event CK from Root"
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

    let mut rops = mapi_private_logon_rops("alice");
    append_rop_open_folder(&mut rops, 0, 1, crate::mapi::identity::CALENDAR_FOLDER_ID);
    rops.extend_from_slice(&[
        0x7e, 0x00, 0x01, 0x02, 0x01, // RopSynchronizationOpenCollector, contents.
        0x72, 0x00, 0x02, 0x03, 0x01, // RopSynchronizationImportMessageChange.
    ]);
    rops.extend_from_slice(&4u16.to_le_bytes());
    rops.extend_from_slice(&import_values);
    append_rop_set_properties(&mut rops, 3, 4, &appointment_values);
    append_rop_save_changes_message_with_flags(&mut rops, 3, 3, 0x08);
    append_rop_get_properties_specific(&mut rops, 3, &[PID_TAG_CHANGE_KEY]);

    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(
                &rops,
                &[u32::MAX, u32::MAX, u32::MAX, u32::MAX],
            )),
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
        last_get_properties_binary_value(&response_rops, 3),
        imported_change_key,
        "same-buffer GetPropertiesSpecific returned a folder CK instead of the committed Event CK"
    );
    assert!(!contains_bytes(&response_rops, &root_change_key));
    assert_eq!(
        response_handles[3], response_handles[1],
        "SaveChangesMessage response handle must be the containing Calendar folder"
    );
}

#[tokio::test]
async fn mapi_over_http_get_properties_rejects_unassigned_numeric_handle() {
    // [MS-OXCROPS] section 3.2.5.4 requires ecNullObject for a Server object
    // handle that was never assigned. It must not fall through to Root-folder
    // property projection.
    let service = ExchangeService::new(FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    });
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert(
        "cookie",
        HeaderValue::from_str(&mapi_cookie_header(&connect)).unwrap(),
    );
    for (index, rops) in [
        {
            let mut rops = Vec::new();
            append_rop_get_properties_specific(&mut rops, 0, &[PID_TAG_CHANGE_KEY]);
            rops
        },
        vec![0x08, 0x00, 0x00, 0x00, 0x10, 0x01, 0x00], // RopGetPropertiesAll.
        vec![0x09, 0x00, 0x00],                         // RopGetPropertiesList.
    ]
    .into_iter()
    .enumerate()
    {
        renew_mapi_request_id(&mut execute_headers);
        let rop_id = rops[0];
        let response = service
            .handle_mapi(
                MapiEndpoint::Emsmdb,
                &execute_headers,
                &execute_body(&rop_buffer(&rops, &[0x1234_5678])),
            )
            .await
            .unwrap();
        let response_rops = response_rops_from_execute_response(response).await;
        assert_eq!(
            response_rops,
            [rop_id, 0x00, 0xB9, 0x04, 0x00, 0x00],
            "property ROP {index} accepted an unassigned numeric handle"
        );
    }
}

#[tokio::test]
async fn mapi_over_http_fai_save_then_get_change_key_keeps_same_input_handle_in_buffer() {
    // Outlook 16 trace 202607241721 imports Inbox FAI, then sends three
    // SetProperties ROPs followed by RopSaveChangesMessage 0x0C
    // (response=0,input=0) and RopGetPropertiesSpecific 0x07 (input=0) for
    // PidTagChangeKey 0x65E20102 in one single-handle buffer.
    // [MS-OXCROPS] sections 1.3.2 and 2.2.6.3 distinguish the input and
    // response handle tables, while [MS-OXCFXICS] section 3.3.4.3.3.2.2.2
    // requires the post-save property read to return the saved Message state.
    let imported_message_id = crate::mapi::identity::mapi_store_id(0x0df8_974b_7f67);
    let imported_source_key = crate::mapi::identity::source_key_for_object_id(imported_message_id);
    let imported_change_key = [
        0xf8, 0x0c, 0x74, 0x3a, 0xc0, 0xfa, 0x02, 0x41, 0xa9, 0x01, 0x08, 0x7c, 0xed, 0x77, 0xf5,
        0xce, 0x00, 0x00, 0x04, 0x14,
    ];
    let mut imported_predecessor_change_list = vec![imported_change_key.len() as u8];
    imported_predecessor_change_list.extend_from_slice(&imported_change_key);

    let account = FakeStore::account();
    let imported_counter =
        crate::mapi::identity::global_counter_from_store_id(imported_message_id).unwrap();
    let store = FakeStore {
        session: Some(account.clone()),
        mapi_local_replica_ranges: Arc::new(Mutex::new(vec![(
            account.account_id,
            imported_counter,
            imported_counter + 1,
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
        test_filetime("2026-07-24", "15:18"),
    );
    append_mapi_binary_property(&mut import_values, PID_TAG_CHANGE_KEY, &imported_change_key);
    append_mapi_binary_property(
        &mut import_values,
        PID_TAG_PREDECESSOR_CHANGE_LIST,
        &imported_predecessor_change_list,
    );

    let mut import_rops = mapi_private_logon_rops("alice");
    append_rop_open_folder(
        &mut import_rops,
        0,
        1,
        crate::mapi::identity::INBOX_FOLDER_ID,
    );
    import_rops.extend_from_slice(&[
        0x7e, 0x00, 0x01, 0x02, 0x01, // RopSynchronizationOpenCollector, contents.
        0x72, 0x00, 0x02, 0x03, 0x10, // RopSynchronizationImportMessageChange, FAI.
    ]);
    import_rops.extend_from_slice(&4u16.to_le_bytes());
    import_rops.extend_from_slice(&import_values);

    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(
                &import_rops,
                &[u32::MAX, u32::MAX, u32::MAX, u32::MAX],
            )),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = response_bytes(response).await;
    let (response_rops, response_handles) = response_rops_and_handles_from_execute_body(&body);
    assert!(contains_bytes(&response_rops, &[0xFE, 0x00, 0, 0, 0, 0]));
    assert!(
        contains_bytes(&response_rops, &[0x72, 0x03, 0, 0, 0, 0]),
        "ImportMessageChange failed: {response_rops:02x?}"
    );
    let inbox_handle = response_handles[1];
    let imported_message_handle = response_handles[3];

    let mut first_batch = Vec::new();
    append_mapi_utf16_property(
        &mut first_batch,
        PID_TAG_MESSAGE_CLASS_W,
        "IPM.ExtendedRule.Message",
    );
    append_mapi_utf16_property(&mut first_batch, PID_TAG_SUBJECT_W, "Junk E-mail Rule");
    append_mapi_i32_property(&mut first_batch, PID_TAG_MESSAGE_FLAGS, 0x449);
    append_mapi_i32_property(&mut first_batch, 0x0017_0003, 1); // PidTagImportance.

    let mut second_batch = Vec::new();
    append_mapi_bool_property(&mut second_batch, 0x0E1F_000B, true); // PidTagRtfInSync.

    let mut third_batch = Vec::new();
    append_mapi_utf16_property(&mut third_batch, 0x003D_001F, ""); // PidTagSubjectPrefix.
    append_mapi_utf16_property(
        &mut third_batch,
        PID_TAG_NORMALIZED_SUBJECT_W,
        "Junk E-mail Rule",
    );

    let mut save_rops = Vec::new();
    append_rop_set_properties(&mut save_rops, 0, 3, &first_batch);
    append_rop_set_properties(&mut save_rops, 0, 1, &second_batch);
    append_rop_set_properties(&mut save_rops, 0, 2, &third_batch);
    append_rop_save_changes_message_with_flags(&mut save_rops, 0, 0, 0x08);
    append_rop_get_properties_specific(&mut save_rops, 0, &[PID_TAG_CHANGE_KEY]);

    renew_mapi_request_id(&mut execute_headers);
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&save_rops, &[imported_message_handle])),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = response_bytes(response).await;
    let (response_rops, response_handles) = response_rops_and_handles_from_execute_body(&body);
    assert!(
        contains_bytes(&response_rops, &[0x0c, 0x00, 0, 0, 0, 0]),
        "SaveChangesMessage failed: {response_rops:02x?}"
    );
    let mut expected_get_properties = vec![0x07, 0x00, 0, 0, 0, 0, 0];
    expected_get_properties.extend_from_slice(&(imported_change_key.len() as u16).to_le_bytes());
    expected_get_properties.extend_from_slice(&imported_change_key);
    assert!(
        contains_bytes(&response_rops, &expected_get_properties),
        "GetPropertiesSpecific after Save must read the imported FAI ChangeKey from the Message handle: {response_rops:02x?}"
    );
    assert_eq!(
        response_handles,
        [inbox_handle],
        "the final response handle table must still project the containing Inbox folder"
    );

    // Outlook 16 trace 202607252141 then reuses the open persisted
    // IPM.ExtendedRule.Message, updates its condition stream, and issues
    // SetProperties > SaveChangesMessage > GetPropertiesSpecific. The
    // post-save PidTagChangeKey must already be the committed value in that
    // buffer, not the pre-Execute snapshot value. [MS-OXCMSG] section 3.2.5.3
    // and [MS-OXCFXICS] sections 2.2.1.2.7 and 3.3.4.3.3.2.2.2.
    let rule_condition = b"extended-rule-condition";
    let mut stream_rops = vec![0x2B, 0x00, 0x00, 0x01]; // RopOpenStream.
    stream_rops.extend_from_slice(&0x0E9A_0102u32.to_le_bytes());
    stream_rops.push(1);
    stream_rops.extend_from_slice(&[0x2F, 0x00, 0x01]); // RopSetStreamSize.
    stream_rops.extend_from_slice(&(rule_condition.len() as u64).to_le_bytes());
    stream_rops.extend_from_slice(&[0x2D, 0x00, 0x01]); // RopWriteStream.
    stream_rops.extend_from_slice(&(rule_condition.len() as u16).to_le_bytes());
    stream_rops.extend_from_slice(rule_condition);
    stream_rops.extend_from_slice(&[0x5D, 0x00, 0x01]); // RopCommitStream.
    renew_mapi_request_id(&mut execute_headers);
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(
                &stream_rops,
                &[imported_message_handle, u32::MAX],
            )),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let mut update_values = Vec::new();
    append_mapi_utf16_property(&mut update_values, 0x003D_001F, "");
    append_mapi_utf16_property(
        &mut update_values,
        PID_TAG_NORMALIZED_SUBJECT_W,
        "Junk E-mail Rule",
    );
    let mut update_save_get_rops = Vec::new();
    append_rop_set_properties(&mut update_save_get_rops, 0, 0, &update_values);
    append_rop_save_changes_message_with_flags(&mut update_save_get_rops, 0, 0, 0x09);
    append_rop_get_properties_specific(
        &mut update_save_get_rops,
        0,
        &[PID_TAG_CHANGE_KEY, 0x0E0B_0102],
    );
    renew_mapi_request_id(&mut execute_headers);
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(
                &update_save_get_rops,
                &[imported_message_handle],
            )),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = response_bytes(response).await;
    let (response_rops, _) = response_rops_and_handles_from_execute_body(&body);
    let same_buffer_change_key = last_get_properties_binary_value(&response_rops, 0);

    let mut next_get_rops = Vec::new();
    append_rop_get_properties_specific(&mut next_get_rops, 0, &[PID_TAG_CHANGE_KEY, 0x0E0B_0102]);
    renew_mapi_request_id(&mut execute_headers);
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&next_get_rops, &[imported_message_handle])),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = response_bytes(response).await;
    let (response_rops, _) = response_rops_and_handles_from_execute_body(&body);
    let next_request_change_key = last_get_properties_binary_value(&response_rops, 0);
    assert_ne!(
        next_request_change_key, imported_change_key,
        "the saved FAI must receive a new server ChangeKey"
    );
    assert_eq!(
        same_buffer_change_key, next_request_change_key,
        "same-buffer and next-request reads must observe the same committed ChangeKey"
    );

    let mut save_release_rops = Vec::new();
    append_rop_save_changes_message_with_flags(&mut save_release_rops, 0, 0, 0x08);
    save_release_rops.extend_from_slice(&[0x01, 0x00, 0x00]); // RopRelease.
    renew_mapi_request_id(&mut execute_headers);
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&save_release_rops, &[imported_message_handle])),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = response_bytes(response).await;
    let (response_rops, response_handles) = response_rops_and_handles_from_execute_body(&body);
    assert!(contains_bytes(&response_rops, &[0x0c, 0x00, 0, 0, 0, 0]));
    assert_eq!(
        response_handles,
        [0],
        "a later Release must not let deferred parent projection resurrect the slot"
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
    let mut rops = mapi_private_logon_rops("alice");
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
            &execute_body(&rop_buffer(
                &rops,
                &[u32::MAX, u32::MAX, u32::MAX, u32::MAX],
            )),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = response_bytes(response).await;
    let (response_rops, response_handles) = response_rops_and_handles_from_execute_body(&body);
    assert!(contains_bytes(&response_rops, &[0xFE, 0x00, 0, 0, 0, 0]));
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
