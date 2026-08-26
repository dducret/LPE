use super::*;

#[derive(Debug, PartialEq, Eq)]
struct ProjectedCalendarVersion {
    change_number: Vec<u8>,
    change_key: Vec<u8>,
    predecessor_change_list: Vec<u8>,
    last_modification_time: u64,
}

fn read_projected_calendar_version(
    response_rops: &[u8],
    mut offset: usize,
) -> ProjectedCalendarVersion {
    let change_number = response_rops[offset..offset + 8].to_vec();
    offset += 8;
    let change_key = read_rop_binary_u16(response_rops, &mut offset)
        .expect("Calendar ChangeKey")
        .to_vec();
    let predecessor_change_list = read_rop_binary_u16(response_rops, &mut offset)
        .expect("Calendar predecessor list")
        .to_vec();
    let last_modification_time = u64::from_le_bytes(
        response_rops[offset..offset + 8]
            .try_into()
            .expect("Calendar last-modification time"),
    );
    ProjectedCalendarVersion {
        change_number,
        change_key,
        predecessor_change_list,
        last_modification_time,
    }
}

#[tokio::test]
async fn mapi_over_http_custom_calendar_contents_and_open_use_same_durable_version() {
    let account = FakeStore::account();
    let collection = FakeStore::collection(
        "versioned-custom-calendar",
        "calendar",
        "Versioned Custom Calendar",
    );
    let event_id = Uuid::parse_str("43555354-4f4d-4341-8c45-4e4441520001").unwrap();
    let event_object_id = crate::mapi::identity::mapi_store_id(0x0022_3344_5566);
    let change_number = 0x0055_6677_8899;
    let mut change_key = Uuid::parse_str("da7a2b47-d5ec-4de8-9c5c-b888dd6a95d9")
        .unwrap()
        .as_bytes()
        .to_vec();
    change_key.extend_from_slice(&[0, 0, 0, 0, 0, 0x2a]);
    let predecessor_change_list = [vec![change_key.len() as u8], change_key.clone()].concat();
    let last_modification_time = mapi_mailstore::filetime_from_rfc3339_utc("2026-08-25T18:42:00Z");
    let event_version = MapiEventVersion {
        event_id,
        canonical_modseq: 7,
        change_number,
        search_key: None,
        change_key: change_key.clone(),
        predecessor_change_list: predecessor_change_list.clone(),
        last_modification_time,
        created_at: "2026-08-25T17:00:00Z".to_string(),
        updated_at: "2026-08-25T18:42:00Z".to_string(),
    };
    let store = FakeStore {
        session: Some(account.clone()),
        calendar_collections: Arc::new(Mutex::new(vec![collection.clone()])),
        events: Arc::new(Mutex::new(vec![AccessibleEvent {
            id: event_id,
            uid: "custom-calendar-version-regression".to_string(),
            collection_id: collection.id.clone(),
            owner_account_id: account.account_id,
            owner_email: account.email.clone(),
            owner_display_name: account.display_name.clone(),
            rights: FakeStore::rights(),
            date: "2026-08-25".to_string(),
            time: "17:00".to_string(),
            time_zone: "UTC".to_string(),
            duration_minutes: 30,
            all_day: false,
            status: "confirmed".to_string(),
            sequence: 0,
            recurrence_rule: String::new(),
            recurrence_json: "{}".to_string(),
            recurrence_exceptions_json: "[]".to_string(),
            title: "Durable version in custom Calendar".to_string(),
            location: String::new(),
            organizer_json: "{}".to_string(),
            attendees: String::new(),
            attendees_json: String::new(),
            notes: String::new(),
            body_html: String::new(),
        }])),
        event_versions: Arc::new(Mutex::new(HashMap::from([(event_id, 7)]))),
        mapi_identities: Arc::new(Mutex::new(HashMap::from([(event_id, event_object_id)]))),
        mapi_identity_change_numbers: Arc::new(Mutex::new(HashMap::from([(
            event_id,
            change_number,
        )]))),
        mapi_identity_change_keys: Arc::new(Mutex::new(HashMap::from([(
            event_id,
            change_key.clone(),
        )]))),
        mapi_identity_predecessor_change_lists: Arc::new(Mutex::new(HashMap::from([(
            event_id,
            predecessor_change_list.clone(),
        )]))),
        mapi_identity_last_modification_times: Arc::new(Mutex::new(HashMap::from([(
            event_id,
            last_modification_time,
        )]))),
        mapi_event_identity_versions: Arc::new(Mutex::new(HashMap::from([(
            event_id,
            event_version,
        )]))),
        ..Default::default()
    };
    let custom_folder_id = store
        .fetch_or_allocate_mapi_identities(
            account.account_id,
            &crate::mapi_store::collaboration_folder_identity_requests(
                &[],
                std::slice::from_ref(&collection),
                &[],
            ),
        )
        .await
        .unwrap()
        .remove(0)
        .object_id;
    let service = ExchangeService::new(store);
    let (mut execute_headers, logon_handle) = mapi_connect_with_private_logon(&service).await;
    let version_columns = [
        0x67A4_0014u32,
        0x65E2_0102u32,
        0x65E3_0102u32,
        0x3008_0040u32,
    ];

    let mut table_rops = Vec::new();
    append_rop_open_folder(&mut table_rops, 0, 1, custom_folder_id);
    table_rops.extend_from_slice(&[0x05, 0x00, 0x01, 0x02, 0x00]);
    table_rops.extend_from_slice(&[0x12, 0x00, 0x02, 0x00]);
    table_rops.extend_from_slice(&(version_columns.len() as u16).to_le_bytes());
    for column in version_columns {
        table_rops.extend_from_slice(&column.to_le_bytes());
    }
    table_rops.extend_from_slice(&[0x15, 0x00, 0x02, 0x00, 0x01]);
    table_rops.extend_from_slice(&1u16.to_le_bytes());
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(
                &table_rops,
                &[logon_handle, u32::MAX, u32::MAX],
            )),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let table_response = response_rops_from_execute_response(response).await;
    let query_rows_offset = 8 + 10 + 7;
    assert_eq!(
        &table_response[query_rows_offset..query_rows_offset + 9],
        &[0x15, 0x02, 0, 0, 0, 0, 0x02, 0x01, 0]
    );
    assert_eq!(table_response[query_rows_offset + 9], 0);
    let table_version = read_projected_calendar_version(&table_response, query_rows_offset + 10);

    renew_mapi_request_id(&mut execute_headers);
    let mut open_rops = Vec::new();
    append_rop_open_folder(&mut open_rops, 0, 1, custom_folder_id);
    append_rop_open_message(&mut open_rops, 1, 2, custom_folder_id, event_object_id);
    append_rop_get_properties_specific(&mut open_rops, 2, &version_columns);
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&open_rops, &[logon_handle, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let open_response = response_rops_from_execute_response(response).await;
    let open_row = mapi_get_properties_specific_standard_row_offset(&open_response, 2)
        .expect("OpenMessage Calendar version row");
    let open_version = read_projected_calendar_version(&open_response, open_row + 1);

    let expected = ProjectedCalendarVersion {
        change_number: mapi_wire_id_bytes(crate::mapi::identity::mapi_store_id(change_number))
            .to_vec(),
        change_key,
        predecessor_change_list,
        last_modification_time,
    };
    assert_eq!(open_version, expected);
    assert_eq!(table_version, open_version);
}
