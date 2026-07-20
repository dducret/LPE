use super::*;

fn empty_appointment_tombstone() -> Vec<u8> {
    let mut tombstone = Vec::new();
    tombstone.extend_from_slice(&0xBEDE_AFCDu32.to_le_bytes()); // Identifier.
    tombstone.extend_from_slice(&0x14u32.to_le_bytes()); // HeaderSize.
    tombstone.extend_from_slice(&0x03u32.to_le_bytes()); // Version.
    tombstone.extend_from_slice(&0u32.to_le_bytes()); // RecordsCount.
    tombstone.extend_from_slice(&0x14u32.to_le_bytes()); // RecordsSize.
    tombstone
}

#[tokio::test]
async fn mapi_over_http_local_freebusy_accepts_outlook_tombstone_maintenance_sequence() {
    // Outlook 16.0.20131 emitted this sequence in the 202607201648 replay:
    // it removes the deprecated PidTagScheduleInfoFreeBusy property, then
    // creates an empty PidTagScheduleInfoAppointmentTombstone stream in five
    // four-byte writes. [MS-OXOPFFB] section 2.2.1.4.3 requires the former to
    // be ignored. The latter is the empty structure from [MS-OXOCAL] sections
    // 2.2.12.5 and 2.2.12.5.1; LocalFreebusy remains a projection of canonical
    // LPE calendar state rather than a second persisted calendar store.
    const PID_TAG_SCHEDULE_INFO_APPOINTMENT_TOMBSTONE: u32 = 0x686A_0102;
    const PID_TAG_SCHEDULE_INFO_FREE_BUSY: u32 = 0x686C_0102;

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
    append_rop_open_message(
        &mut rops,
        0,
        1,
        crate::mapi::identity::FREEBUSY_DATA_FOLDER_ID,
        local_freebusy_id,
    );
    let mut deprecated_freebusy = Vec::new();
    append_mapi_binary_property(
        &mut deprecated_freebusy,
        PID_TAG_SCHEDULE_INFO_FREE_BUSY,
        b"ignored",
    );
    append_rop_set_properties(&mut rops, 1, 1, &deprecated_freebusy);
    rops.extend_from_slice(&[0x7A, 0x00, 0x01]); // RopDeletePropertiesNoReplicate.
    rops.extend_from_slice(&1u16.to_le_bytes());
    rops.extend_from_slice(&PID_TAG_SCHEDULE_INFO_FREE_BUSY.to_le_bytes());
    rops.extend_from_slice(&[0x2B, 0x00, 0x01, 0x02]); // RopOpenStream, create.
    rops.extend_from_slice(&PID_TAG_SCHEDULE_INFO_APPOINTMENT_TOMBSTONE.to_le_bytes());
    rops.push(0x02);

    let empty_tombstones = empty_appointment_tombstone();
    for chunk in empty_tombstones.chunks_exact(4) {
        rops.extend_from_slice(&[0x2D, 0x00, 0x02]); // RopWriteStream.
        rops.extend_from_slice(&(chunk.len() as u16).to_le_bytes());
        rops.extend_from_slice(chunk);
    }
    rops.extend_from_slice(&[0x01, 0x00, 0x02]); // RopRelease stream.
    append_rop_save_changes_message_with_flags(&mut rops, 1, 1, 0x0A);
    rops.extend_from_slice(&[0x2B, 0x00, 0x01, 0x02]); // RopOpenStream, read.
    rops.extend_from_slice(&PID_TAG_SCHEDULE_INFO_APPOINTMENT_TOMBSTONE.to_le_bytes());
    rops.push(0x00);
    rops.extend_from_slice(&[0x2C, 0x00, 0x02]); // RopReadStream.
    rops.extend_from_slice(&(empty_tombstones.len() as u16).to_le_bytes());
    append_rop_get_properties_specific(
        &mut rops,
        1,
        &[PID_TAG_MESSAGE_CLASS_W, PID_TAG_SOURCE_KEY],
    );

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

    for (rop_id, handle_index) in [
        (0x03, 0x01),
        (0x0A, 0x01),
        (0x7A, 0x01),
        (0x2B, 0x02),
        (0x2D, 0x02),
        (0x0C, 0x01),
        (0x2C, 0x02),
        (0x07, 0x01),
    ] {
        assert!(
            contains_bytes(&response_rops, &[rop_id, handle_index, 0, 0, 0, 0]),
            "ROP 0x{rop_id:02x} failed: {response_rops:02x?}"
        );
    }
    assert_eq!(
        response_rops
            .windows(8)
            .filter(|window| *window == [0x2D, 0x02, 0, 0, 0, 0, 4, 0])
            .count(),
        5,
        "all five Outlook stream writes must report WrittenSize=4: {response_rops:02x?}"
    );
    let mut read_response = vec![0x2C, 0x02, 0, 0, 0, 0];
    read_response.extend_from_slice(&(empty_tombstones.len() as u16).to_le_bytes());
    read_response.extend_from_slice(&empty_tombstones);
    assert!(
        contains_bytes(&response_rops, &read_response),
        "saved empty tombstone must be re-readable as a valid structure: {response_rops:02x?}"
    );
}

#[tokio::test]
async fn mapi_over_http_local_freebusy_rejects_nonempty_tombstone_without_canonical_mapping() {
    // [MS-OXOCAL] sections 2.2.12.5 and 2.2.12.5.1 define nonempty records
    // as deleted-meeting state. LPE must not acknowledge that state until it
    // can map it to the canonical calendar instead of silently discarding it.
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
    append_rop_open_message(
        &mut rops,
        0,
        1,
        crate::mapi::identity::FREEBUSY_DATA_FOLDER_ID,
        local_freebusy_id,
    );
    rops.extend_from_slice(&[0x2B, 0x00, 0x01, 0x02]); // RopOpenStream, create.
    rops.extend_from_slice(&PID_TAG_SCHEDULE_INFO_APPOINTMENT_TOMBSTONE.to_le_bytes());
    rops.push(0x02);
    let mut unsupported_nonempty = empty_appointment_tombstone();
    unsupported_nonempty[12..16].copy_from_slice(&1u32.to_le_bytes()); // RecordsCount.
    rops.extend_from_slice(&[0x2D, 0x00, 0x02]); // RopWriteStream.
    rops.extend_from_slice(&(unsupported_nonempty.len() as u16).to_le_bytes());
    rops.extend_from_slice(&unsupported_nonempty);
    rops.extend_from_slice(&[0x01, 0x00, 0x02]); // RopRelease stream.
    append_rop_save_changes_message_with_flags(&mut rops, 1, 1, 0x0A);

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
        contains_bytes(&response_rops, &[0x2D, 0x02, 0, 0, 0, 0, 20, 0]),
        "the stream transaction must accept the complete write: {response_rops:02x?}"
    );
    assert!(
        contains_bytes(&response_rops, &[0x0C, 0x01, 0x57, 0x00, 0x07, 0x80]),
        "Save must reject a nonempty tombstone with MAPI_E_INVALID_PARAMETER: {response_rops:02x?}"
    );
}

#[tokio::test]
async fn mapi_over_http_local_freebusy_rejects_created_but_incomplete_tombstone() {
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
    append_rop_open_message(
        &mut rops,
        0,
        1,
        crate::mapi::identity::FREEBUSY_DATA_FOLDER_ID,
        local_freebusy_id,
    );
    rops.extend_from_slice(&[0x2B, 0x00, 0x01, 0x02]); // RopOpenStream, create.
    rops.extend_from_slice(&PID_TAG_SCHEDULE_INFO_APPOINTMENT_TOMBSTONE.to_le_bytes());
    rops.push(0x02);
    rops.extend_from_slice(&[0x01, 0x00, 0x02]); // RopRelease untouched stream.
    append_rop_save_changes_message_with_flags(&mut rops, 1, 1, 0x0A);

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
        contains_bytes(&response_rops, &[0x0C, 0x01, 0x57, 0x00, 0x07, 0x80]),
        "Save must reject a created but incomplete tombstone: {response_rops:02x?}"
    );
}

#[tokio::test]
async fn mapi_over_http_local_freebusy_direct_tombstone_set_is_staged_until_save() {
    // [MS-OXCPRPT] sections 3.2.5.4 and 3.2.5.13 require a Message property
    // mutation to be visible through the same open object before Save. A
    // direct SetProperties for the optional [MS-OXOCAL] 2.2.12.5 tombstone
    // must therefore follow the same transaction as its stream form instead
    // of being acknowledged and discarded.
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
    let staged_tombstone = vec![0xA5, 0x5A, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06];
    let mut property_values = Vec::new();
    append_mapi_binary_property(
        &mut property_values,
        PID_TAG_SCHEDULE_INFO_APPOINTMENT_TOMBSTONE,
        &staged_tombstone,
    );

    let mut rops = Vec::new();
    append_rop_open_message(
        &mut rops,
        0,
        1,
        crate::mapi::identity::FREEBUSY_DATA_FOLDER_ID,
        local_freebusy_id,
    );
    append_rop_set_properties(&mut rops, 1, 1, &property_values);
    rops.extend_from_slice(&[0x2B, 0x00, 0x01, 0x02]); // RopOpenStream, read.
    rops.extend_from_slice(&PID_TAG_SCHEDULE_INFO_APPOINTMENT_TOMBSTONE.to_le_bytes());
    rops.push(0x00);
    rops.extend_from_slice(&[0x2C, 0x00, 0x02]); // RopReadStream.
    rops.extend_from_slice(&(staged_tombstone.len() as u16).to_le_bytes());
    append_rop_get_properties_specific(
        &mut rops,
        1,
        &[PID_TAG_SCHEDULE_INFO_APPOINTMENT_TOMBSTONE],
    );
    append_rop_save_changes_message_with_flags(&mut rops, 1, 1, 0x0A);

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
        contains_bytes(&response_rops, &[0x0A, 0x01, 0, 0, 0, 0]),
        "direct tombstone SetProperties failed: {response_rops:02x?}"
    );
    let mut read_response = vec![0x2C, 0x02, 0, 0, 0, 0];
    read_response.extend_from_slice(&(staged_tombstone.len() as u16).to_le_bytes());
    read_response.extend_from_slice(&staged_tombstone);
    assert!(
        contains_bytes(&response_rops, &read_response),
        "OpenStream on the same Message must expose the staged value: {response_rops:02x?}"
    );
    let mut get_response = vec![0x07, 0x01, 0, 0, 0, 0, 0];
    get_response.extend_from_slice(&(staged_tombstone.len() as u16).to_le_bytes());
    get_response.extend_from_slice(&staged_tombstone);
    assert!(
        contains_bytes(&response_rops, &get_response),
        "GetPropertiesSpecific on the same Message must expose the staged value: {response_rops:02x?}"
    );
    assert!(
        contains_bytes(&response_rops, &[0x0C, 0x01, 0x57, 0x00, 0x07, 0x80]),
        "Save must reject the incomplete staged value: {response_rops:02x?}"
    );
}

#[tokio::test]
async fn mapi_over_http_local_freebusy_rejects_unmodeled_mixed_set_without_partial_stage() {
    // [MS-OXCPRPT] section 3.2.5.4 does not permit acknowledging a property
    // that the server then discards. Validate the complete batch before
    // staging 0x686A so a rejected companion property cannot leave a partial
    // Message transaction behind.
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
    let mut property_values = Vec::new();
    append_mapi_binary_property(
        &mut property_values,
        PID_TAG_SCHEDULE_INFO_APPOINTMENT_TOMBSTONE,
        &[0xA5, 0x5A],
    );
    append_mapi_utf16_property(
        &mut property_values,
        PID_TAG_SUBJECT_W,
        "must not be discarded",
    );

    let mut rops = Vec::new();
    append_rop_open_message(
        &mut rops,
        0,
        1,
        crate::mapi::identity::FREEBUSY_DATA_FOLDER_ID,
        local_freebusy_id,
    );
    append_rop_set_properties(&mut rops, 1, 2, &property_values);
    append_rop_save_changes_message_with_flags(&mut rops, 1, 1, 0x0A);

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
    assert!(
        contains_bytes(&response_rops, &[0x0A, 0x01, 0x02, 0x01, 0x04, 0x80]),
        "the unmodeled mixed SetProperties must be rejected: {response_rops:02x?}"
    );
    assert!(
        contains_bytes(&response_rops, &[0x0C, 0x01, 0, 0, 0, 0]),
        "rejected SetProperties must not leave the incomplete 0x686A value staged: {response_rops:02x?}"
    );
}
