use super::*;

const PID_TAG_RECIPIENT_PROPOSED: u32 = 0x5FE1_000B;
const PID_TAG_RECIPIENT_PROPOSED_START_TIME: u32 = 0x5FE3_0040;
const PID_TAG_RECIPIENT_PROPOSED_END_TIME: u32 = 0x5FE4_0040;
const PID_TAG_RECIPIENT_TRACK_STATUS_TIME: u32 = 0x5FFB_0040;

#[tokio::test]
async fn calendar_event_open_read_and_reload_project_counter_proposal() {
    let account = FakeStore::account();
    let event_id = Uuid::parse_str("82828282-8282-4282-8282-828282828282").unwrap();
    let store = FakeStore {
        session: Some(account.clone()),
        calendar_collections: Arc::new(Mutex::new(vec![FakeStore::collection(
            "default", "calendar", "Calendar",
        )])),
        events: Arc::new(Mutex::new(vec![AccessibleEvent {
            id: event_id,
            uid: "probe-8@example.test".to_string(),
            collection_id: "default".to_string(),
            owner_account_id: account.account_id,
            owner_email: account.email.clone(),
            owner_display_name: account.display_name.clone(),
            rights: FakeStore::rights(),
            date: "2026-08-26".to_string(),
            time: "09:00".to_string(),
            time_zone: "UTC".to_string(),
            duration_minutes: 30,
            all_day: false,
            status: "confirmed".to_string(),
            sequence: 3,
            recurrence_rule: String::new(),
            recurrence_json: "{}".to_string(),
            recurrence_exceptions_json: "[]".to_string(),
            title: "Probe 8".to_string(),
            location: String::new(),
            organizer_json: format!(
                r#"{{"email":"{}","common_name":"{}"}}"#,
                account.email, account.display_name
            ),
            attendees: "Denis Ducret".to_string(),
            attendees_json: format!(
                r#"{{"organizer":{{"email":"{}","common_name":"{}"}},"attendees":[{{"email":"denis.ducret@sdic.ch","common_name":"Denis Ducret","role":"REQ-PARTICIPANT","partstat":"declined","rsvp":true,"proposed_start":"2026-08-26T10:00:00Z","proposed_end":"2026-08-26T10:30:00Z","counter_proposal":true}}]}}"#,
                account.email, account.display_name
            ),
            notes: String::new(),
            body_html: String::new(),
        }])),
        mapi_calendar_recipient_response_times: Arc::new(Mutex::new(vec![
            MapiCalendarRecipientResponseTime {
                event_id,
                attendee_email: "denis.ducret@sdic.ch".to_string(),
                response_sent_at: "2026-08-26T07:15:00Z".to_string(),
            },
        ])),
        ..Default::default()
    };
    let calendar_folder_id = durable_special_folder_id_for_test(
        &store,
        account.account_id,
        crate::mapi::identity::CALENDAR_FOLDER_ID,
    )
    .await;
    let service = ExchangeService::new(store);
    let (mut execute_headers, logon_handle) = mapi_connect_with_private_logon(&service).await;

    let mut open_rops = Vec::new();
    append_rop_open_folder(&mut open_rops, 0, 1, calendar_folder_id);
    append_rop_open_message(
        &mut open_rops,
        1,
        2,
        calendar_folder_id,
        test_mapi_uuid_id(&event_id),
    );
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&open_rops, &[logon_handle, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    let body = response_bytes(response).await;
    let (response_rops, handles) = response_rops_and_handles_from_execute_body(&body);
    let open_response = response_rops[8..].to_vec();
    let (recipient_count, columns, open_rows) = decode_open_message_recipients(&response_rops[8..]);

    assert_eq!(
        response_rops[14], 1,
        "Calendar Event must advertise named properties"
    );
    assert_eq!(recipient_count, 2);
    assert_eq!(open_rows.len(), 2);
    assert_eq!(
        &columns[6..9],
        &[
            PID_TAG_RECIPIENT_PROPOSED,
            PID_TAG_RECIPIENT_PROPOSED_START_TIME,
            PID_TAG_RECIPIENT_PROPOSED_END_TIME,
        ]
    );
    assert_no_counter_proposal_values(&columns, &open_rows[0]);
    assert_counter_proposal_values(&columns, &open_rows[1]);
    assert_recipient_track_status_time(&columns, &open_rows[0], None);
    assert_recipient_track_status_time(
        &columns,
        &open_rows[1],
        Some(crate::mapi_mailstore::filetime_from_rfc3339_utc(
            "2026-08-26T07:15:00Z",
        )),
    );

    let mut read_rops = vec![0x0F, 0, 2];
    read_rops.extend_from_slice(&0u32.to_le_bytes());
    read_rops.extend_from_slice(&0u16.to_le_bytes());
    renew_mapi_request_id(&mut execute_headers);
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&read_rops, &handles)),
        )
        .await
        .unwrap();
    let response_rops = response_rops_from_execute_response(response).await;
    let read_rows = decode_read_recipient_rows(&response_rops);

    assert_eq!(read_rows.len(), 2);
    assert_eq!(read_rows[0].0, 0);
    assert_eq!(read_rows[1].0, 1);
    assert_no_counter_proposal_values(&columns, &read_rows[0].1);
    assert_counter_proposal_values(&columns, &read_rows[1].1);
    assert_recipient_track_status_time(&columns, &read_rows[0].1, None);
    assert_recipient_track_status_time(
        &columns,
        &read_rows[1].1,
        Some(crate::mapi_mailstore::filetime_from_rfc3339_utc(
            "2026-08-26T07:15:00Z",
        )),
    );

    renew_mapi_request_id(&mut execute_headers);
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&[0x10, 0, 2, 0, 0], &handles)),
        )
        .await
        .unwrap();
    let reload_response = response_rops_from_execute_response(response).await;

    assert_eq!(&reload_response[..2], &[0x10, 0x02]);
    assert_eq!(
        &reload_response[2..],
        &open_response[2..],
        "saved Event reload must preserve the exact OpenMessage information payload"
    );
}

#[tokio::test]
async fn meeting_response_reload_preserves_full_open_message_payload() {
    let account = FakeStore::account();
    let inbox_id = Uuid::parse_str("85858585-8585-4585-8585-858585858585").unwrap();
    let message_id = Uuid::parse_str("86868686-8686-4686-8686-868686868686").unwrap();
    let mut email = FakeStore::email(
        &message_id.to_string(),
        &inbox_id.to_string(),
        "inbox",
        "New Time Proposed: Probe 8",
    );
    email.from_address = "denis.ducret@sdic.ch".to_string();
    email.from_display = Some("Denis Ducret".to_string());
    email.to = vec![lpe_storage::JmapEmailAddress {
        address: account.email.clone(),
        display_name: Some(account.display_name.clone()),
    }];
    email.calendar_meeting_response = Some(lpe_storage::CalendarMeetingResponse {
        method: "COUNTER".to_string(),
        transport_attachment_id: None,
        server_processed: true,
        organizer: Some(lpe_storage::CalendarMeetingIdentity {
            email: account.email.clone(),
            display_name: account.display_name.clone(),
        }),
        attendee_email: "denis.ducret@sdic.ch".to_string(),
        attendee_name: "Denis Ducret".to_string(),
        partstat: "declined".to_string(),
        uid: "probe-8@example.test".to_string(),
        response_sent_at: Some("2026-08-26T07:15:00Z".to_string()),
        meeting_start: Some("2026-08-26T09:00:00Z".to_string()),
        meeting_end: Some("2026-08-26T09:30:00Z".to_string()),
        meeting_location: Some("Room 8".to_string()),
        meeting_sequence: Some(3),
        proposed_start: Some("2026-08-26T10:00:00Z".to_string()),
        proposed_end: Some("2026-08-26T10:30:00Z".to_string()),
        original_start: Some("2026-08-26T09:00:00Z".to_string()),
        original_end: Some("2026-08-26T09:30:00Z".to_string()),
    });
    let store = FakeStore {
        session: Some(account.clone()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            &inbox_id.to_string(),
            "inbox",
            "Inbox",
        )])),
        emails: Arc::new(Mutex::new(vec![email])),
        ..Default::default()
    };
    let inbox_folder_id = durable_special_folder_id_for_test(
        &store,
        account.account_id,
        crate::mapi::identity::INBOX_FOLDER_ID,
    )
    .await;
    let service = ExchangeService::new(store);
    let (mut execute_headers, logon_handle) = mapi_connect_with_private_logon(&service).await;

    let mut open_rops = Vec::new();
    append_rop_open_folder(&mut open_rops, 0, 1, inbox_folder_id);
    append_rop_open_message(
        &mut open_rops,
        1,
        2,
        inbox_folder_id,
        test_mapi_uuid_id(&message_id),
    );
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&open_rops, &[logon_handle, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    let body = response_bytes(response).await;
    let (response_rops, handles) = response_rops_and_handles_from_execute_body(&body);
    let open_response = response_rops[8..].to_vec();
    let (recipient_count, columns, rows) = decode_open_message_recipients(&open_response);

    assert_eq!(
        open_response[6], 1,
        "response must advertise named properties"
    );
    assert_eq!(recipient_count, 2);
    assert_eq!(columns.len(), 16);
    assert_eq!(rows.len(), 2);

    renew_mapi_request_id(&mut execute_headers);
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&[0x10, 0, 2, 0, 0], &handles)),
        )
        .await
        .unwrap();
    let reload_response = response_rops_from_execute_response(response).await;

    assert_eq!(&reload_response[..2], &[0x10, 0x02]);
    assert_eq!(
        &reload_response[2..],
        &open_response[2..],
        "saved Meeting Response reload must preserve named properties and recipient rows"
    );
}

#[tokio::test]
async fn recipient_free_calendar_event_open_preserves_invariant_recipient_columns() {
    let account = FakeStore::account();
    let event_id = Uuid::parse_str("83838383-8383-4383-8383-838383838383").unwrap();
    let store = FakeStore {
        session: Some(account.clone()),
        calendar_collections: Arc::new(Mutex::new(vec![FakeStore::collection(
            "default", "calendar", "Calendar",
        )])),
        events: Arc::new(Mutex::new(vec![AccessibleEvent {
            id: event_id,
            uid: event_id.to_string(),
            collection_id: "default".to_string(),
            owner_account_id: account.account_id,
            owner_email: account.email,
            owner_display_name: account.display_name,
            rights: FakeStore::rights(),
            date: "2026-08-26".to_string(),
            time: "09:00".to_string(),
            time_zone: "UTC".to_string(),
            duration_minutes: 30,
            all_day: false,
            status: "confirmed".to_string(),
            sequence: 0,
            recurrence_rule: String::new(),
            recurrence_json: "{}".to_string(),
            recurrence_exceptions_json: "[]".to_string(),
            title: "Personal appointment".to_string(),
            location: String::new(),
            organizer_json: "{}".to_string(),
            attendees: String::new(),
            attendees_json: "{}".to_string(),
            notes: String::new(),
            body_html: String::new(),
        }])),
        ..Default::default()
    };
    let calendar_folder_id = durable_special_folder_id_for_test(
        &store,
        account.account_id,
        crate::mapi::identity::CALENDAR_FOLDER_ID,
    )
    .await;
    let service = ExchangeService::new(store);
    let (execute_headers, logon_handle) = mapi_connect_with_private_logon(&service).await;
    let mut open_rops = Vec::new();
    append_rop_open_folder(&mut open_rops, 0, 1, calendar_folder_id);
    append_rop_open_message(
        &mut open_rops,
        1,
        2,
        calendar_folder_id,
        test_mapi_uuid_id(&event_id),
    );
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&open_rops, &[logon_handle, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    let response_rops = response_rops_from_execute_response(response).await;
    let (recipient_count, columns, rows) = decode_open_message_recipients(&response_rops[8..]);

    assert_eq!(recipient_count, 0);
    assert_eq!(columns.len(), 20);
    assert_eq!(
        &columns[6..9],
        &[
            PID_TAG_RECIPIENT_PROPOSED,
            PID_TAG_RECIPIENT_PROPOSED_START_TIME,
            PID_TAG_RECIPIENT_PROPOSED_END_TIME,
        ]
    );
    assert!(rows.is_empty());
}

fn assert_no_counter_proposal_values(columns: &[u32], row: &[u8]) {
    let values = decode_recipient_property_values(columns, row);
    for property_tag in [
        PID_TAG_RECIPIENT_PROPOSED,
        PID_TAG_RECIPIENT_PROPOSED_START_TIME,
        PID_TAG_RECIPIENT_PROPOSED_END_TIME,
    ] {
        assert_eq!(values[&property_tag], None);
    }
}

fn assert_counter_proposal_values(columns: &[u32], row: &[u8]) {
    let values = decode_recipient_property_values(columns, row);
    assert_eq!(
        values[&PID_TAG_RECIPIENT_PROPOSED].as_deref(),
        Some(&[1][..])
    );
    assert_eq!(
        u64::from_le_bytes(
            values[&PID_TAG_RECIPIENT_PROPOSED_START_TIME]
                .as_deref()
                .unwrap()
                .try_into()
                .unwrap()
        ),
        crate::mapi_mailstore::filetime_from_rfc3339_utc("2026-08-26T10:00:00Z")
    );
    assert_eq!(
        u64::from_le_bytes(
            values[&PID_TAG_RECIPIENT_PROPOSED_END_TIME]
                .as_deref()
                .unwrap()
                .try_into()
                .unwrap()
        ),
        crate::mapi_mailstore::filetime_from_rfc3339_utc("2026-08-26T10:30:00Z")
    );
}

fn assert_recipient_track_status_time(columns: &[u32], row: &[u8], expected: Option<u64>) {
    let values = decode_recipient_property_values(columns, row);
    let actual = values[&PID_TAG_RECIPIENT_TRACK_STATUS_TIME]
        .as_deref()
        .map(|value| u64::from_le_bytes(value.try_into().unwrap()));
    assert_eq!(actual, expected);
}

fn decode_open_message_recipients(response: &[u8]) -> (u16, Vec<u32>, Vec<Vec<u8>>) {
    assert_eq!(&response[..6], &[0x03, 0x02, 0, 0, 0, 0]);
    let mut offset = 7;
    skip_typed_string(response, &mut offset);
    skip_typed_string(response, &mut offset);
    let recipient_count = read_u16(response, &mut offset);
    let column_count = read_u16(response, &mut offset) as usize;
    let columns = (0..column_count)
        .map(|_| read_u32(response, &mut offset))
        .collect::<Vec<_>>();
    let row_count = response[offset] as usize;
    offset += 1;
    let rows = (0..row_count)
        .map(|_| {
            offset += 5;
            let row_size = read_u16(response, &mut offset) as usize;
            let row = response[offset..offset + row_size].to_vec();
            offset += row_size;
            row
        })
        .collect();
    (recipient_count, columns, rows)
}

fn decode_read_recipient_rows(response: &[u8]) -> Vec<(u32, Vec<u8>)> {
    assert_eq!(&response[..6], &[0x0F, 0x02, 0, 0, 0, 0]);
    let row_count = response[6] as usize;
    let mut offset = 7;
    (0..row_count)
        .map(|_| {
            let row_id = read_u32(response, &mut offset);
            offset += 5;
            let row_size = read_u16(response, &mut offset) as usize;
            let row = response[offset..offset + row_size].to_vec();
            offset += row_size;
            (row_id, row)
        })
        .collect()
}

fn decode_recipient_property_values(columns: &[u32], row: &[u8]) -> HashMap<u32, Option<Vec<u8>>> {
    let recipient_flags = u16::from_le_bytes(row[..2].try_into().unwrap());
    let mut offset = 2;
    if recipient_flags & 0x0008 != 0 {
        skip_utf16z(row, &mut offset);
        skip_utf16z(row, &mut offset);
    } else {
        offset += 2;
        while row[offset] != 0 {
            offset += 1;
        }
        offset += 1;
        skip_utf16z(row, &mut offset);
    }
    assert_eq!(read_u16(row, &mut offset) as usize, columns.len());
    let flagged = row[offset] != 0;
    offset += 1;
    columns
        .iter()
        .map(|property_tag| {
            if flagged {
                let flag = row[offset];
                offset += 1;
                if flag != 0 {
                    assert_eq!(flag, 0x0A);
                    assert_eq!(read_u32(row, &mut offset), 0x8004_010F);
                    return (*property_tag, None);
                }
            }
            let start = offset;
            match property_tag & 0xFFFF {
                0x0003 => offset += 4,
                0x000B => offset += 1,
                0x0040 => offset += 8,
                0x001F => skip_utf16z(row, &mut offset),
                0x0102 => {
                    let size = read_u16(row, &mut offset) as usize;
                    offset += size;
                }
                property_type => panic!("unsupported recipient property type {property_type:#06x}"),
            }
            (*property_tag, Some(row[start..offset].to_vec()))
        })
        .collect()
}

fn skip_typed_string(bytes: &[u8], offset: &mut usize) {
    let string_type = bytes[*offset];
    *offset += 1;
    match string_type {
        0x01 => {}
        0x04 => skip_utf16z(bytes, offset),
        _ => panic!("unexpected TypedString type {string_type:#04x}"),
    }
}

fn skip_utf16z(bytes: &[u8], offset: &mut usize) {
    while bytes[*offset..*offset + 2] != [0, 0] {
        *offset += 2;
    }
    *offset += 2;
}

fn read_u16(bytes: &[u8], offset: &mut usize) -> u16 {
    let value = u16::from_le_bytes(bytes[*offset..*offset + 2].try_into().unwrap());
    *offset += 2;
    value
}

fn read_u32(bytes: &[u8], offset: &mut usize) -> u32 {
    let value = u32::from_le_bytes(bytes[*offset..*offset + 4].try_into().unwrap());
    *offset += 4;
    value
}
