use super::*;

#[tokio::test]
async fn mapi_over_http_exchange_rule_organizer_query_rows_opens_returned_message() {
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

    // Exchange 2016 raw/551 in test1_202608031300.saz restricts the Inbox
    // associated-contents table to this FAI, then raw/554 opens the returned
    // FolderId/MID pair. [MS-OXORULE] section 3.1.4.2.4 identifies it as the
    // Rules Organizer FAI in the Inbox.
    let mut rule_organizer_restriction = vec![0x04, 0x04];
    rule_organizer_restriction.extend_from_slice(&PID_TAG_MESSAGE_CLASS_W.to_le_bytes());
    append_mapi_utf16_property(
        &mut rule_organizer_restriction,
        PID_TAG_MESSAGE_CLASS_W,
        "IPM.RuleOrganizer",
    );

    let mut query_rops = Vec::new();
    append_rop_open_folder(
        &mut query_rops,
        0,
        1,
        crate::mapi::identity::INBOX_FOLDER_ID,
    );
    query_rops.extend_from_slice(&[
        0x05, 0x00, 0x01, 0x02, 0x02, // RopGetContentsTable, associated.
        0x12, 0x00, 0x02, 0x00, // RopSetColumns.
    ]);
    let columns = [
        0x6748_0014, // PidTagFolderId.
        0x674A_0014, // PidTagMid.
        0x674D_0014, // PidTagInstId.
        0x674E_0003, // PidTagInstanceNum.
        PID_TAG_MESSAGE_CLASS_W,
        0x3008_0040, // PidTagLastModificationTime.
    ];
    query_rops.extend_from_slice(&(columns.len() as u16).to_le_bytes());
    for column in columns {
        query_rops.extend_from_slice(&column.to_le_bytes());
    }
    query_rops.extend_from_slice(&[0x14, 0x00, 0x02, 0x00]); // RopRestrict.
    query_rops.extend_from_slice(&(rule_organizer_restriction.len() as u16).to_le_bytes());
    query_rops.extend_from_slice(&rule_organizer_restriction);
    query_rops.extend_from_slice(&[0x13, 0x00, 0x02, 0x00]); // RopSortTable.
    query_rops.extend_from_slice(&1u16.to_le_bytes());
    query_rops.extend_from_slice(&0u16.to_le_bytes());
    query_rops.extend_from_slice(&0u16.to_le_bytes());
    query_rops.extend_from_slice(&0x3008_0040u32.to_le_bytes());
    query_rops.push(0);
    query_rops.extend_from_slice(&[0x18, 0x00, 0x02, 0x00]); // RopSeekRow, beginning.
    query_rops.extend_from_slice(&0i32.to_le_bytes());
    query_rops.push(1);
    query_rops.extend_from_slice(&[0x15, 0x00, 0x02, 0x00, 0x01]); // RopQueryRows.
    query_rops.extend_from_slice(&1u16.to_le_bytes());

    let query_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&query_rops, &[1, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    assert_eq!(query_response.status(), StatusCode::OK);
    assert_eq!(query_response.headers().get("x-responsecode").unwrap(), "0");
    let query_response_body = response_bytes(query_response).await;
    let (query_response_rops, query_response_handles) =
        response_rops_and_handles_from_execute_body(&query_response_body);
    let folder_handle = query_response_handles[1];
    let table_handle = query_response_handles[2];
    let query_marker = [0x15, 0x02, 0, 0, 0, 0, 0x02, 0x01, 0x00];
    let query_offset = query_response_rops
        .windows(query_marker.len())
        .position(|window| window == query_marker)
        .expect("Rule Organizer QueryRows response contains one final row");
    let row_offset = query_offset + query_marker.len();
    assert_eq!(query_response_rops[row_offset], 0);

    let returned_folder_wire_id = query_response_rops[row_offset + 1..row_offset + 9].to_vec();
    let returned_message_wire_id = query_response_rops[row_offset + 9..row_offset + 17].to_vec();
    let returned_folder_id =
        crate::mapi::identity::object_id_from_wire_id(&returned_folder_wire_id)
            .expect("QueryRows FolderId is a valid MAPI wire identifier");
    let returned_message_id =
        crate::mapi::identity::object_id_from_wire_id(&returned_message_wire_id)
            .expect("QueryRows MID is a valid MAPI wire identifier");
    assert_ne!(returned_folder_id, 0);
    assert_eq!(
        returned_message_id,
        crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFED)
    );
    assert_eq!(
        &query_response_rops[row_offset + 17..row_offset + 25],
        &returned_message_id.to_le_bytes()
    );
    assert!(contains_bytes(
        &query_response_rops,
        &utf16z("IPM.RuleOrganizer")
    ));

    renew_mapi_request_id(&mut execute_headers);
    let mut open_rops = vec![0x01, 0x00, 0x02]; // RopRelease the associated table.
    append_rop_open_message(
        &mut open_rops,
        1,
        2,
        returned_folder_id,
        returned_message_id,
    );
    append_rop_get_properties_specific(
        &mut open_rops,
        2,
        &[PID_TAG_MESSAGE_CLASS_W, PID_TAG_SUBJECT_W],
    );
    assert!(contains_bytes(&open_rops, &returned_folder_wire_id));
    assert!(contains_bytes(&open_rops, &returned_message_wire_id));

    let open_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&open_rops, &[1, folder_handle, table_handle])),
        )
        .await
        .unwrap();
    assert_eq!(open_response.status(), StatusCode::OK);
    assert_eq!(open_response.headers().get("x-responsecode").unwrap(), "0");
    let open_response_body = response_bytes(open_response).await;
    let (open_response_rops, open_response_handles) =
        response_rops_and_handles_from_execute_body(&open_response_body);
    let message_handle = open_response_handles[2];
    assert!(contains_bytes(
        &open_response_rops,
        &[0x03, 0x02, 0, 0, 0, 0]
    ));
    let property_row_offset =
        mapi_get_properties_specific_standard_row_offset(&open_response_rops, 2).unwrap();
    assert_eq!(open_response_rops[property_row_offset], 0);
    let message_class = utf16z("IPM.RuleOrganizer");
    let subject = utf16z("Outlook Rules Organizer");
    let message_class_offset = property_row_offset + 1;
    let subject_offset = message_class_offset + message_class.len();
    assert_eq!(
        &open_response_rops[message_class_offset..subject_offset],
        message_class.as_slice()
    );
    assert_eq!(
        &open_response_rops[subject_offset..subject_offset + subject.len()],
        subject.as_slice()
    );

    // Exchange 2016 test1_202608031300.saz raw/554 opens this stream, requests
    // 4096 bytes, and returns exactly 66. [MS-OXORULE] section 3.1.4.2.4.
    renew_mapi_request_id(&mut execute_headers);
    let mut stream_rops = vec![0x2B, 0x00, 0x02, 0x03]; // RopOpenStream, read.
    stream_rops.extend_from_slice(&0x6802_0102u32.to_le_bytes());
    stream_rops.push(0x00);
    stream_rops.extend_from_slice(&[0x2C, 0x00, 0x03]); // RopReadStream.
    stream_rops.extend_from_slice(&4096u16.to_le_bytes());
    let stream_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(
                &stream_rops,
                &[1, folder_handle, message_handle, u32::MAX],
            )),
        )
        .await
        .unwrap();
    assert_eq!(stream_response.status(), StatusCode::OK);
    assert_eq!(
        stream_response.headers().get("x-responsecode").unwrap(),
        "0"
    );
    let stream_response_rops = response_rops_from_execute_response(stream_response).await;
    assert!(contains_bytes(
        &stream_response_rops,
        &[0x2B, 0x03, 0, 0, 0, 0, 66, 0, 0, 0]
    ));
    let mut expected_read = vec![0x2C, 0x03, 0, 0, 0, 0];
    expected_read.extend_from_slice(&66u16.to_le_bytes());
    expected_read.extend_from_slice(&[
        0x14, 0x00, 0x00, 0x00, 0x14, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x01, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0xFE, 0xFF,
    ]);
    assert!(contains_bytes(&stream_response_rops, &expected_read));
}
