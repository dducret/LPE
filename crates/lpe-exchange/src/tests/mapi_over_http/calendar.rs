use super::*;

#[tokio::test]
async fn mapi_over_http_calendar_custom_properties_do_not_persist_for_existing_guarded_event() {
    let account = FakeStore::account();
    let event_id = Uuid::parse_str("cececece-cece-cece-cece-cececece1234").unwrap();
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
            owner_email: account.email.clone(),
            owner_display_name: account.display_name.clone(),
            rights: FakeStore::rights(),
            date: "2026-06-04".to_string(),
            time: "09:00".to_string(),
            time_zone: "UTC".to_string(),
            duration_minutes: 30,
            all_day: false,
            status: "confirmed".to_string(),
            sequence: 0,
            recurrence_rule: String::new(),
            recurrence_json: "{}".to_string(),
            recurrence_exceptions_json: "[]".to_string(),
            title: "Restart custom calendar".to_string(),
            location: String::new(),
            organizer_json: "{}".to_string(),
            attendees: String::new(),
            attendees_json: String::new(),
            notes: String::new(),
            body_html: String::new(),
        }])),
        ..Default::default()
    };
    let stored_custom_values = store.mapi_custom_property_values.clone();
    let custom_tag = 0x8001_001F;
    let service = ExchangeService::new(store.clone());
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert(
        "cookie",
        HeaderValue::from_str(&mapi_cookie_header(&connect)).unwrap(),
    );

    let mut property_values = Vec::new();
    append_mapi_utf16_property(
        &mut property_values,
        custom_tag,
        "calendar restart opaque value",
    );
    let mut set_rops = Vec::new();
    append_rop_open_folder(&mut set_rops, 0, 1, test_mapi_folder_id(16));
    append_rop_open_message(
        &mut set_rops,
        1,
        2,
        test_mapi_folder_id(16),
        test_mapi_uuid_id(&event_id),
    );
    append_rop_set_properties(&mut set_rops, 2, 1, &property_values);
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&set_rops, &[1, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    let _response_rops = response_rops_from_execute_response(response).await;
    assert!(stored_custom_values.lock().unwrap().is_empty());

    let restarted = ExchangeService::new(store);
    let connect = restarted
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let mut restarted_headers = mapi_headers("Execute");
    restarted_headers.insert(
        "cookie",
        HeaderValue::from_str(&mapi_cookie_header(&connect)).unwrap(),
    );
    let mut get_rops = Vec::new();
    append_rop_open_folder(&mut get_rops, 0, 1, test_mapi_folder_id(16));
    append_rop_open_message(
        &mut get_rops,
        1,
        2,
        test_mapi_folder_id(16),
        test_mapi_uuid_id(&event_id),
    );
    append_rop_get_properties_specific(&mut get_rops, 2, &[custom_tag]);
    let response = restarted
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &restarted_headers,
            &execute_body(&rop_buffer(&get_rops, &[1, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    let response_rops = response_rops_from_execute_response(response).await;

    assert!(!contains_bytes(
        &response_rops,
        &utf16z("calendar restart opaque value")
    ));
    assert!(stored_custom_values.lock().unwrap().is_empty());
}

#[tokio::test]
async fn mapi_over_http_calendar_create_persists_then_existing_handle_is_hidden() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        calendar_collections: Arc::new(Mutex::new(vec![FakeStore::collection(
            "default", "calendar", "Calendar",
        )])),
        ..Default::default()
    };
    let events = store.events.clone();
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = HeaderValue::from_str(
        connect
            .headers()
            .get("set-cookie")
            .unwrap()
            .to_str()
            .unwrap()
            .split(';')
            .next()
            .unwrap(),
    )
    .unwrap();

    let mut property_values = Vec::new();
    append_mapi_utf16_property(&mut property_values, 0x0037_001F, "RCA Calendar");
    append_mapi_i64_property(
        &mut property_values,
        0x0060_0040,
        test_filetime("2026-05-04", "09:30"),
    );
    append_mapi_i64_property(
        &mut property_values,
        0x0061_0040,
        test_filetime("2026-05-04", "10:15"),
    );
    append_mapi_utf16_property(&mut property_values, 0x3FFB_001F, "Room 1");
    append_mapi_utf16_property(&mut property_values, 0x1000_001F, "Agenda");
    append_mapi_utf16_property(&mut property_values, 0x1013_001F, "<p>Agenda</p>");
    append_mapi_i32_property(&mut property_values, 0x8205_0003, 1);
    append_mapi_bool_property(&mut property_values, 0x8215_000B, true);
    append_mapi_binary_property(
        &mut property_values,
        0x8216_0102,
        &test_daily_calendar_recur_blob(),
    );
    append_mapi_utf16_property(&mut property_values, 0x0C1A_001F, "Alice Organizer");
    append_mapi_utf16_property(
        &mut property_values,
        0x0C1F_001F,
        "Alice.Organizer@Example.Test",
    );
    append_mapi_utf16_property(&mut property_values, 0x0E04_001F, "Bob Attendee");
    let mut rops = Vec::new();
    append_rop_create_message(&mut rops, 0, 1, test_mapi_folder_id(16));
    append_rop_set_properties(&mut rops, 1, 12, &property_values);
    append_rop_save_changes_message(&mut rops, 1, 1);
    append_rop_save_changes_message(&mut rops, 1, 1);
    append_rop_get_properties_specific(&mut rops, 1, &[0x0037_001F, 0x0060_0040, 0x0061_0040]);
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", cookie.clone());
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(&response_rops, &[0x0C, 0x01, 0, 0, 0, 0]));
    assert!(contains_bytes(
        &response_rops,
        &[0x0C, 0x01, 0x0F, 0x01, 0x04, 0x80]
    ));
    assert!(contains_bytes(
        &response_rops,
        &[0x07, 0x01, 0x0F, 0x01, 0x04, 0x80]
    ));
    {
        let stored = events.lock().unwrap();
        assert_eq!(stored.len(), 1);
        assert_eq!(stored[0].title, "RCA Calendar");
        assert_eq!(stored[0].date, "2026-05-04");
        assert_eq!(stored[0].time, "09:30");
        assert_eq!(stored[0].duration_minutes, 45);
        assert_eq!(stored[0].location, "Room 1");
        assert_eq!(stored[0].notes, "Agenda");
        assert_eq!(stored[0].body_html, "<p>Agenda</p>");
        assert!(stored[0].all_day);
        assert_eq!(stored[0].status, "tentative");
        assert_eq!(stored[0].attendees, "Bob Attendee");
        assert!(stored[0]
            .organizer_json
            .contains("alice.organizer@example.test"));
        assert!(stored[0].attendees_json.contains("Bob Attendee"));
        assert_eq!(stored[0].recurrence_rule, "FREQ=DAILY;COUNT=3");
        assert_eq!(
            stored[0].recurrence_json,
            r#"{"frequency":"daily","count":3}"#
        );
    }
}

#[tokio::test]
async fn mapi_over_http_empty_advertised_calendar_create_uses_default_collection() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let events = store.events.clone();
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = HeaderValue::from_str(
        connect
            .headers()
            .get("set-cookie")
            .unwrap()
            .to_str()
            .unwrap()
            .split(';')
            .next()
            .unwrap(),
    )
    .unwrap();

    let mut property_values = Vec::new();
    append_mapi_utf16_property(&mut property_values, 0x0037_001F, "First Calendar Item");
    append_mapi_i64_property(
        &mut property_values,
        0x0060_0040,
        test_filetime("2026-06-01", "08:00"),
    );
    append_mapi_i64_property(
        &mut property_values,
        0x0061_0040,
        test_filetime("2026-06-01", "08:30"),
    );

    let mut rops = Vec::new();
    append_rop_create_message(&mut rops, 0, 1, test_mapi_folder_id(16));
    append_rop_set_properties(&mut rops, 1, 3, &property_values);
    append_rop_save_changes_message(&mut rops, 1, 1);
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", cookie);
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[1, u32::MAX])),
        )
        .await
        .unwrap();
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(
        !response_rops
            .windows(4)
            .any(|window| window == 0x8004_0102u32.to_le_bytes())
            && !response_rops
                .windows(4)
                .any(|window| window == 0x8004_010Fu32.to_le_bytes()),
        "calendar create returned an error: {response_rops:02x?}"
    );

    let stored = events.lock().unwrap();
    assert_eq!(stored.len(), 1);
    assert_eq!(stored[0].collection_id, "default");
    assert_eq!(stored[0].title, "First Calendar Item");
    assert_eq!(stored[0].date, "2026-06-01");
    assert_eq!(stored[0].time, "08:00");
    assert_eq!(stored[0].duration_minutes, 30);
}

#[tokio::test]
async fn mapi_over_http_advertised_calendar_update_delete_do_not_mutate_existing_guarded_event() {
    let account = FakeStore::account();
    let event_id = Uuid::parse_str("cccccccc-cccc-cccc-cccc-cccccccc0001").unwrap();
    let store = FakeStore {
        session: Some(account.clone()),
        events: Arc::new(Mutex::new(vec![AccessibleEvent {
            id: event_id,
            uid: event_id.to_string(),
            collection_id: "default".to_string(),
            owner_account_id: account.account_id,
            owner_email: account.email.clone(),
            owner_display_name: account.display_name.clone(),
            rights: FakeStore::rights(),
            date: "2026-06-02".to_string(),
            time: "13:00".to_string(),
            time_zone: "UTC".to_string(),
            duration_minutes: 45,
            all_day: false,
            status: "confirmed".to_string(),
            sequence: 0,
            recurrence_rule: String::new(),
            recurrence_json: "{}".to_string(),
            recurrence_exceptions_json: "[]".to_string(),
            title: "Implicit default calendar".to_string(),
            location: "Room 1".to_string(),
            organizer_json: "{}".to_string(),
            attendees: String::new(),
            attendees_json: String::new(),
            notes: String::new(),
            body_html: String::new(),
        }])),
        ..Default::default()
    };
    let events = store.events.clone();
    let deleted_events = store.deleted_events.clone();
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

    let mut update_values = Vec::new();
    append_mapi_utf16_property(&mut update_values, 0x0037_001F, "Updated implicit calendar");
    append_mapi_utf16_property(&mut update_values, 0x3FFB_001F, "Room 2");
    let mut update_rops = Vec::new();
    append_rop_open_folder(&mut update_rops, 0, 1, test_mapi_folder_id(16));
    append_rop_open_message(
        &mut update_rops,
        1,
        2,
        test_mapi_folder_id(16),
        test_mapi_uuid_id(&event_id),
    );
    append_rop_set_properties(&mut update_rops, 2, 2, &update_values);
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&update_rops, &[1, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    let _response_rops = response_rops_from_execute_response(response).await;
    {
        let stored = events.lock().unwrap();
        assert_eq!(stored[0].title, "Implicit default calendar");
        assert_eq!(stored[0].location, "Room 1");
    }

    let mut delete_rops = Vec::new();
    append_rop_open_folder(&mut delete_rops, 0, 1, test_mapi_folder_id(16));
    append_rop_delete_messages(&mut delete_rops, 1, &[test_mapi_uuid_id(&event_id)]);
    renew_mapi_request_id(&mut execute_headers);
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&delete_rops, &[1, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    let _response_rops = response_rops_from_execute_response(response).await;
    assert_eq!(events.lock().unwrap().len(), 1);
    assert!(deleted_events.lock().unwrap().is_empty());
}

#[tokio::test]
async fn mapi_over_http_calendar_create_rejects_malformed_recurrence_without_event() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        calendar_collections: Arc::new(Mutex::new(vec![FakeStore::collection(
            "default", "calendar", "Calendar",
        )])),
        ..Default::default()
    };
    let events = store.events.clone();
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = HeaderValue::from_str(
        connect
            .headers()
            .get("set-cookie")
            .unwrap()
            .to_str()
            .unwrap()
            .split(';')
            .next()
            .unwrap(),
    )
    .unwrap();

    let mut property_values = Vec::new();
    append_mapi_utf16_property(&mut property_values, 0x0037_001F, "Rejected recurrence");
    append_mapi_i64_property(
        &mut property_values,
        0x0060_0040,
        test_filetime("2026-05-04", "09:30"),
    );
    append_mapi_binary_property(&mut property_values, 0x8216_0102, &[1, 2, 3]);
    let mut rops = Vec::new();
    append_rop_create_message(&mut rops, 0, 1, test_mapi_folder_id(16));
    append_rop_set_properties(&mut rops, 1, 3, &property_values);
    append_rop_save_changes_message(&mut rops, 1, 1);
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", cookie);
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    let response_rops = response_rops_from_execute_response(response).await;

    assert!(contains_bytes(
        &response_rops,
        &0x8004_0102u32.to_le_bytes()
    ));
    assert!(events.lock().unwrap().is_empty());
}

#[tokio::test]
async fn mapi_over_http_calendar_mixed_reminder_and_malformed_recurrence_has_no_side_effect() {
    let account = FakeStore::account();
    let event_id = Uuid::parse_str("cdcdcdcd-cdcd-cdcd-cdcd-cdcdcdcdcdcd").unwrap();
    let reminders = Arc::new(Mutex::new(Vec::new()));
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
            owner_email: account.email.clone(),
            owner_display_name: account.display_name.clone(),
            rights: FakeStore::rights(),
            date: "2026-05-04".to_string(),
            time: "09:30".to_string(),
            time_zone: "UTC".to_string(),
            duration_minutes: 30,
            all_day: false,
            status: "confirmed".to_string(),
            sequence: 0,
            recurrence_rule: String::new(),
            recurrence_json: "{}".to_string(),
            recurrence_exceptions_json: "[]".to_string(),
            title: "Reminder recurrence guard".to_string(),
            location: String::new(),
            organizer_json: "{}".to_string(),
            attendees: String::new(),
            attendees_json: String::new(),
            notes: String::new(),
            body_html: String::new(),
        }])),
        reminders: reminders.clone(),
        ..Default::default()
    };
    let events = store.events.clone();
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

    let mut property_values = Vec::new();
    property_values.extend_from_slice(&0x8503_000Bu32.to_le_bytes());
    property_values.push(1);
    append_mapi_i64_property(
        &mut property_values,
        0x8560_0040,
        mapi_mailstore::filetime_from_rfc3339_utc("2026-05-04T09:00:00Z") as i64,
    );
    append_mapi_binary_property(&mut property_values, 0x8216_0102, &[1, 2, 3]);

    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, test_mapi_folder_id(16));
    append_rop_open_message(
        &mut rops,
        1,
        2,
        test_mapi_folder_id(16),
        test_mapi_uuid_id(&event_id),
    );
    append_rop_set_properties(&mut rops, 2, 3, &property_values);

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
    assert!(contains_bytes(
        &response_rops,
        &0x8004_0102u32.to_le_bytes()
    ));
    assert!(reminders.lock().unwrap().is_empty());
    let stored = events.lock().unwrap();
    assert_eq!(stored[0].recurrence_rule, "");
    assert_eq!(stored[0].title, "Reminder recurrence guard");
}

#[tokio::test]
async fn mapi_over_http_calendar_create_canonicalizes_bounded_meeting_request() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        calendar_collections: Arc::new(Mutex::new(vec![FakeStore::collection(
            "default", "calendar", "Calendar",
        )])),
        ..Default::default()
    };
    let events = store.events.clone();
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = HeaderValue::from_str(
        connect
            .headers()
            .get("set-cookie")
            .unwrap()
            .to_str()
            .unwrap()
            .split(';')
            .next()
            .unwrap(),
    )
    .unwrap();

    let mut property_values = Vec::new();
    append_mapi_utf16_property(
        &mut property_values,
        0x001A_001F,
        "IPM.Schedule.Meeting.Request",
    );
    append_mapi_utf16_property(&mut property_values, 0x0037_001F, "Meeting request");
    append_mapi_i64_property(
        &mut property_values,
        0x0060_0040,
        test_filetime("2026-05-04", "09:30"),
    );
    append_mapi_i64_property(
        &mut property_values,
        0x0061_0040,
        test_filetime("2026-05-04", "10:00"),
    );
    append_mapi_utf16_property(&mut property_values, 0x0C1A_001F, "Alice Organizer");
    append_mapi_utf16_property(&mut property_values, 0x0C1F_001F, "alice@example.test");
    append_mapi_utf16_property(&mut property_values, 0x0E04_001F, "Bob Attendee");
    let mut rops = Vec::new();
    append_rop_create_message(&mut rops, 0, 1, test_mapi_folder_id(16));
    append_rop_set_properties(&mut rops, 1, 7, &property_values);
    append_rop_save_changes_message(&mut rops, 1, 1);
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", cookie);
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    let response_rops = response_rops_from_execute_response(response).await;

    assert!(!contains_bytes(
        &response_rops,
        &0x8004_0102u32.to_le_bytes()
    ));
    let stored = events.lock().unwrap();
    assert_eq!(stored.len(), 1);
    assert_eq!(stored[0].title, "Meeting request");
    assert_eq!(stored[0].date, "2026-05-04");
    assert_eq!(stored[0].time, "09:30");
    assert_eq!(stored[0].duration_minutes, 30);
    assert!(stored[0].organizer_json.contains("alice@example.test"));
    assert!(stored[0].attendees_json.contains("Bob Attendee"));
}

#[tokio::test]
async fn mapi_over_http_calendar_meeting_cancel_is_hidden_for_existing_guarded_event() {
    let account = FakeStore::account();
    let event_id = Uuid::parse_str("cececece-cece-cece-cece-cececececece").unwrap();
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
            owner_email: account.email.clone(),
            owner_display_name: account.display_name.clone(),
            rights: FakeStore::rights(),
            date: "2026-05-04".to_string(),
            time: "09:30".to_string(),
            time_zone: "UTC".to_string(),
            duration_minutes: 30,
            all_day: false,
            status: "confirmed".to_string(),
            sequence: 0,
            recurrence_rule: String::new(),
            recurrence_json: "{}".to_string(),
            recurrence_exceptions_json: "[]".to_string(),
            title: "Meeting to cancel".to_string(),
            location: String::new(),
            organizer_json: "{}".to_string(),
            attendees: String::new(),
            attendees_json: String::new(),
            notes: String::new(),
            body_html: String::new(),
        }])),
        ..Default::default()
    };
    let events = store.events.clone();
    let deleted_events = store.deleted_events.clone();
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

    let mut property_values = Vec::new();
    append_mapi_utf16_property(
        &mut property_values,
        0x001A_001F,
        "IPM.Schedule.Meeting.Canceled",
    );
    append_mapi_utf16_property(&mut property_values, 0x0037_001F, "Cancelled meeting");

    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, test_mapi_folder_id(16));
    append_rop_open_message(
        &mut rops,
        1,
        2,
        test_mapi_folder_id(16),
        test_mapi_uuid_id(&event_id),
    );
    append_rop_set_properties(&mut rops, 2, 2, &property_values);

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
    assert!(contains_bytes(
        &response_rops,
        &0x8004_0102u32.to_le_bytes()
    ));
    assert_eq!(events.lock().unwrap().len(), 1);
    assert!(deleted_events.lock().unwrap().is_empty());
}

#[tokio::test]
async fn mapi_over_http_calendar_meeting_cancel_rejects_binary_payload_without_side_effect() {
    let account = FakeStore::account();
    let event_id = Uuid::parse_str("cececece-cece-cece-cece-000000000001").unwrap();
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
            owner_email: account.email.clone(),
            owner_display_name: account.display_name.clone(),
            rights: FakeStore::rights(),
            date: "2026-05-04".to_string(),
            time: "09:30".to_string(),
            time_zone: "UTC".to_string(),
            duration_minutes: 30,
            all_day: false,
            status: "confirmed".to_string(),
            sequence: 0,
            recurrence_rule: String::new(),
            recurrence_json: "{}".to_string(),
            recurrence_exceptions_json: "[]".to_string(),
            title: "Meeting cancel guard".to_string(),
            location: String::new(),
            organizer_json: "{}".to_string(),
            attendees: String::new(),
            attendees_json: String::new(),
            notes: String::new(),
            body_html: String::new(),
        }])),
        ..Default::default()
    };
    let events = store.events.clone();
    let deleted_events = store.deleted_events.clone();
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

    let mut property_values = Vec::new();
    append_mapi_utf16_property(
        &mut property_values,
        0x001A_001F,
        "IPM.Schedule.Meeting.Canceled",
    );
    append_mapi_binary_property(&mut property_values, 0x8216_0102, &[1, 2, 3]);

    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, test_mapi_folder_id(16));
    append_rop_open_message(
        &mut rops,
        1,
        2,
        test_mapi_folder_id(16),
        test_mapi_uuid_id(&event_id),
    );
    append_rop_set_properties(&mut rops, 2, 2, &property_values);

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
    assert!(contains_bytes(
        &response_rops,
        &0x8004_0102u32.to_le_bytes()
    ));
    assert_eq!(events.lock().unwrap().len(), 1);
    assert!(deleted_events.lock().unwrap().is_empty());
}

#[tokio::test]
async fn mapi_over_http_calendar_meeting_response_is_hidden_for_existing_guarded_event() {
    let account = FakeStore::account();
    let event_id = Uuid::parse_str("cfcfcfcf-cfcf-cfcf-cfcf-cfcfcfcfcfcf").unwrap();
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
            owner_email: account.email.clone(),
            owner_display_name: account.display_name.clone(),
            rights: FakeStore::rights(),
            date: "2026-05-04".to_string(),
            time: "09:30".to_string(),
            time_zone: "UTC".to_string(),
            duration_minutes: 30,
            all_day: false,
            status: "confirmed".to_string(),
            sequence: 0,
            recurrence_rule: String::new(),
            recurrence_json: "{}".to_string(),
            recurrence_exceptions_json: "[]".to_string(),
            title: "Meeting response".to_string(),
            location: String::new(),
            organizer_json: "{}".to_string(),
            attendees: "Bob".to_string(),
            attendees_json: r#"{"attendees":[{"email":"bob@example.test","common_name":"Bob","role":"REQ-PARTICIPANT","partstat":"needs-action","rsvp":true}]}"#.to_string(),
            notes: String::new(),
            body_html: String::new(),
        }])),
        ..Default::default()
    };
    let events = store.events.clone();
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

    let mut property_values = Vec::new();
    append_mapi_utf16_property(
        &mut property_values,
        0x001A_001F,
        "IPM.Schedule.Meeting.Resp.Pos",
    );
    append_mapi_utf16_property(&mut property_values, 0x0C1A_001F, "Bob");
    append_mapi_utf16_property(&mut property_values, 0x0C1F_001F, "bob@example.test");

    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, test_mapi_folder_id(16));
    append_rop_open_message(
        &mut rops,
        1,
        2,
        test_mapi_folder_id(16),
        test_mapi_uuid_id(&event_id),
    );
    append_rop_set_properties(&mut rops, 2, 3, &property_values);

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
    assert!(contains_bytes(
        &response_rops,
        &0x8004_0102u32.to_le_bytes()
    ));
    let stored = events.lock().unwrap();
    assert_eq!(stored[0].attendees, "Bob");
    assert!(stored[0]
        .attendees_json
        .contains(r#""partstat":"needs-action""#));
}

#[tokio::test]
async fn mapi_over_http_calendar_meeting_response_rejects_binary_payload_without_side_effect() {
    let account = FakeStore::account();
    let event_id = Uuid::parse_str("d0d0d0d0-d0d0-d0d0-d0d0-d0d0d0d0d0d0").unwrap();
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
            owner_email: account.email.clone(),
            owner_display_name: account.display_name.clone(),
            rights: FakeStore::rights(),
            date: "2026-05-04".to_string(),
            time: "09:30".to_string(),
            time_zone: "UTC".to_string(),
            duration_minutes: 30,
            all_day: false,
            status: "confirmed".to_string(),
            sequence: 0,
            recurrence_rule: String::new(),
            recurrence_json: "{}".to_string(),
            recurrence_exceptions_json: "[]".to_string(),
            title: "Unsupported meeting response".to_string(),
            location: String::new(),
            organizer_json: "{}".to_string(),
            attendees: "Bob".to_string(),
            attendees_json: r#"{"attendees":[{"email":"bob@example.test","common_name":"Bob","role":"REQ-PARTICIPANT","partstat":"needs-action","rsvp":true}]}"#.to_string(),
            notes: String::new(),
            body_html: String::new(),
        }])),
        ..Default::default()
    };
    let events = store.events.clone();
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

    let mut property_values = Vec::new();
    append_mapi_utf16_property(
        &mut property_values,
        0x001A_001F,
        "IPM.Schedule.Meeting.Resp.Pos",
    );
    append_mapi_utf16_property(&mut property_values, 0x0C1A_001F, "Bob");
    append_mapi_utf16_property(&mut property_values, 0x0C1F_001F, "bob@example.test");
    append_mapi_binary_property(&mut property_values, 0x8216_0102, &[1, 2, 3]);

    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, test_mapi_folder_id(16));
    append_rop_open_message(
        &mut rops,
        1,
        2,
        test_mapi_folder_id(16),
        test_mapi_uuid_id(&event_id),
    );
    append_rop_set_properties(&mut rops, 2, 4, &property_values);

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
    assert!(contains_bytes(
        &response_rops,
        &0x8004_0102u32.to_le_bytes()
    ));
    let stored = events.lock().unwrap();
    assert!(stored[0]
        .attendees_json
        .contains(r#""partstat":"needs-action""#));
}

#[tokio::test]
async fn mapi_over_http_calendar_attendee_named_properties_are_hidden_for_existing_guarded_event() {
    let account = FakeStore::account();
    let event_id = Uuid::parse_str("d1d1d1d1-d1d1-d1d1-d1d1-d1d1d1d1d1d1").unwrap();
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
            owner_email: account.email.clone(),
            owner_display_name: account.display_name.clone(),
            rights: FakeStore::rights(),
            date: "2026-05-04".to_string(),
            time: "09:30".to_string(),
            time_zone: "UTC".to_string(),
            duration_minutes: 30,
            all_day: false,
            status: "confirmed".to_string(),
            sequence: 0,
            recurrence_rule: String::new(),
            recurrence_json: "{}".to_string(),
            recurrence_exceptions_json: "[]".to_string(),
            title: "Attendee named properties".to_string(),
            location: String::new(),
            organizer_json: "{}".to_string(),
            attendees: String::new(),
            attendees_json: String::new(),
            notes: String::new(),
            body_html: String::new(),
        }])),
        ..Default::default()
    };
    let events = store.events.clone();
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

    let mut property_values = Vec::new();
    append_mapi_utf16_property(&mut property_values, 0x823B_001F, "Bob Required");
    append_mapi_utf16_property(&mut property_values, 0x823C_001F, "Cara Optional");

    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, test_mapi_folder_id(16));
    append_rop_open_message(
        &mut rops,
        1,
        2,
        test_mapi_folder_id(16),
        test_mapi_uuid_id(&event_id),
    );
    append_rop_set_properties(&mut rops, 2, 2, &property_values);

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
    assert!(contains_bytes(
        &response_rops,
        &0x8004_0102u32.to_le_bytes()
    ));
    let stored = events.lock().unwrap();
    assert!(stored[0].attendees.is_empty());
    assert!(stored[0].attendees_json.is_empty());
}

#[tokio::test]
async fn mapi_over_http_calendar_display_cc_is_hidden_for_existing_guarded_event() {
    let account = FakeStore::account();
    let event_id = Uuid::parse_str("d8d8d8d8-d8d8-d8d8-d8d8-d8d8d8d8d8d8").unwrap();
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
            owner_email: account.email.clone(),
            owner_display_name: account.display_name.clone(),
            rights: FakeStore::rights(),
            date: "2026-05-04".to_string(),
            time: "09:30".to_string(),
            time_zone: "UTC".to_string(),
            duration_minutes: 30,
            all_day: false,
            status: "confirmed".to_string(),
            sequence: 0,
            recurrence_rule: String::new(),
            recurrence_json: "{}".to_string(),
            recurrence_exceptions_json: "[]".to_string(),
            title: "Display CC attendees".to_string(),
            location: String::new(),
            organizer_json: "{}".to_string(),
            attendees: String::new(),
            attendees_json: String::new(),
            notes: String::new(),
            body_html: String::new(),
        }])),
        ..Default::default()
    };
    let events = store.events.clone();
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

    let mut property_values = Vec::new();
    append_mapi_utf16_property(&mut property_values, 0x0E04_001F, "Bob Required");
    append_mapi_utf16_property(&mut property_values, 0x0E03_001F, "Cara Optional");

    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, test_mapi_folder_id(16));
    append_rop_open_message(
        &mut rops,
        1,
        2,
        test_mapi_folder_id(16),
        test_mapi_uuid_id(&event_id),
    );
    append_rop_set_properties(&mut rops, 2, 2, &property_values);

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
    assert!(contains_bytes(
        &response_rops,
        &0x8004_0102u32.to_le_bytes()
    ));
    let stored = events.lock().unwrap();
    assert!(stored[0].attendees.is_empty());
    assert!(stored[0].attendees_json.is_empty());
}

#[tokio::test]
async fn mapi_over_http_calendar_time_zone_blob_rejects_without_side_effect() {
    let account = FakeStore::account();
    let event_id = Uuid::parse_str("d2d2d2d2-d2d2-d2d2-d2d2-d2d2d2d2d2d2").unwrap();
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
            owner_email: account.email.clone(),
            owner_display_name: account.display_name.clone(),
            rights: FakeStore::rights(),
            date: "2026-05-04".to_string(),
            time: "09:30".to_string(),
            time_zone: "UTC".to_string(),
            duration_minutes: 30,
            all_day: false,
            status: "confirmed".to_string(),
            sequence: 0,
            recurrence_rule: String::new(),
            recurrence_json: "{}".to_string(),
            recurrence_exceptions_json: "[]".to_string(),
            title: "Timezone blob".to_string(),
            location: String::new(),
            organizer_json: "{}".to_string(),
            attendees: String::new(),
            attendees_json: String::new(),
            notes: String::new(),
            body_html: String::new(),
        }])),
        ..Default::default()
    };
    let events = store.events.clone();
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

    let mut property_values = Vec::new();
    append_mapi_binary_property(&mut property_values, 0x8233_0102, &[1, 2, 3, 4]);

    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, test_mapi_folder_id(16));
    append_rop_open_message(
        &mut rops,
        1,
        2,
        test_mapi_folder_id(16),
        test_mapi_uuid_id(&event_id),
    );
    append_rop_set_properties(&mut rops, 2, 1, &property_values);

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
    assert!(contains_bytes(
        &response_rops,
        &0x8004_0102u32.to_le_bytes()
    ));
    let stored = events.lock().unwrap();
    assert_eq!(stored[0].time_zone, "UTC");
}

#[tokio::test]
async fn mapi_over_http_calendar_time_zone_description_is_hidden_for_existing_guarded_event() {
    let account = FakeStore::account();
    let event_id = Uuid::parse_str("d3d3d3d3-d3d3-d3d3-d3d3-d3d3d3d3d3d3").unwrap();
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
            owner_email: account.email.clone(),
            owner_display_name: account.display_name.clone(),
            rights: FakeStore::rights(),
            date: "2026-05-04".to_string(),
            time: "09:30".to_string(),
            time_zone: "UTC".to_string(),
            duration_minutes: 30,
            all_day: false,
            status: "confirmed".to_string(),
            sequence: 0,
            recurrence_rule: String::new(),
            recurrence_json: "{}".to_string(),
            recurrence_exceptions_json: "[]".to_string(),
            title: "Timezone description".to_string(),
            location: String::new(),
            organizer_json: "{}".to_string(),
            attendees: String::new(),
            attendees_json: String::new(),
            notes: String::new(),
            body_html: String::new(),
        }])),
        ..Default::default()
    };
    let events = store.events.clone();
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

    let mut property_values = Vec::new();
    append_mapi_utf16_property(&mut property_values, 0x8234_001F, "W. Europe Standard Time");

    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, test_mapi_folder_id(16));
    append_rop_open_message(
        &mut rops,
        1,
        2,
        test_mapi_folder_id(16),
        test_mapi_uuid_id(&event_id),
    );
    append_rop_set_properties(&mut rops, 2, 1, &property_values);

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
    assert!(contains_bytes(
        &response_rops,
        &0x8004_0102u32.to_le_bytes()
    ));
    let stored = events.lock().unwrap();
    assert_eq!(stored[0].time_zone, "UTC");
}

#[tokio::test]
async fn mapi_over_http_calendar_whole_start_end_is_hidden_for_existing_guarded_event() {
    let account = FakeStore::account();
    let event_id = Uuid::parse_str("d5d5d5d5-d5d5-d5d5-d5d5-d5d5d5d5d5d5").unwrap();
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
            owner_email: account.email.clone(),
            owner_display_name: account.display_name.clone(),
            rights: FakeStore::rights(),
            date: "2026-05-04".to_string(),
            time: "09:30".to_string(),
            time_zone: "UTC".to_string(),
            duration_minutes: 30,
            all_day: false,
            status: "confirmed".to_string(),
            sequence: 0,
            recurrence_rule: String::new(),
            recurrence_json: "{}".to_string(),
            recurrence_exceptions_json: "[]".to_string(),
            title: "Whole start end".to_string(),
            location: String::new(),
            organizer_json: "{}".to_string(),
            attendees: String::new(),
            attendees_json: String::new(),
            notes: String::new(),
            body_html: String::new(),
        }])),
        ..Default::default()
    };
    let events = store.events.clone();
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

    let mut property_values = Vec::new();
    append_mapi_i64_property(
        &mut property_values,
        0x820D_0040,
        test_filetime("2026-06-01", "13:15"),
    );
    append_mapi_i64_property(
        &mut property_values,
        0x820E_0040,
        test_filetime("2026-06-01", "14:45"),
    );

    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, test_mapi_folder_id(16));
    append_rop_open_message(
        &mut rops,
        1,
        2,
        test_mapi_folder_id(16),
        test_mapi_uuid_id(&event_id),
    );
    append_rop_set_properties(&mut rops, 2, 2, &property_values);

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
    assert!(contains_bytes(
        &response_rops,
        &0x8004_0102u32.to_le_bytes()
    ));
    let stored = events.lock().unwrap();
    assert_eq!(stored[0].date, "2026-05-04");
    assert_eq!(stored[0].time, "09:30");
    assert_eq!(stored[0].duration_minutes, 30);
}

#[tokio::test]
async fn mapi_over_http_calendar_common_start_end_is_hidden_for_existing_guarded_event() {
    let account = FakeStore::account();
    let event_id = Uuid::parse_str("d7d7d7d7-d7d7-d7d7-d7d7-d7d7d7d7d7d7").unwrap();
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
            owner_email: account.email.clone(),
            owner_display_name: account.display_name.clone(),
            rights: FakeStore::rights(),
            date: "2026-05-04".to_string(),
            time: "09:30".to_string(),
            time_zone: "UTC".to_string(),
            duration_minutes: 30,
            all_day: false,
            status: "confirmed".to_string(),
            sequence: 0,
            recurrence_rule: String::new(),
            recurrence_json: "{}".to_string(),
            recurrence_exceptions_json: "[]".to_string(),
            title: "Common start end".to_string(),
            location: String::new(),
            organizer_json: "{}".to_string(),
            attendees: String::new(),
            attendees_json: String::new(),
            notes: String::new(),
            body_html: String::new(),
        }])),
        ..Default::default()
    };
    let events = store.events.clone();
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

    let mut property_values = Vec::new();
    append_mapi_i64_property(
        &mut property_values,
        0x8516_0040,
        test_filetime("2026-06-02", "08:00"),
    );
    append_mapi_i64_property(
        &mut property_values,
        0x8517_0040,
        test_filetime("2026-06-02", "09:30"),
    );

    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, test_mapi_folder_id(16));
    append_rop_open_message(
        &mut rops,
        1,
        2,
        test_mapi_folder_id(16),
        test_mapi_uuid_id(&event_id),
    );
    append_rop_set_properties(&mut rops, 2, 2, &property_values);

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
    assert!(contains_bytes(
        &response_rops,
        &0x8004_0102u32.to_le_bytes()
    ));
    let stored = events.lock().unwrap();
    assert_eq!(stored[0].date, "2026-05-04");
    assert_eq!(stored[0].time, "09:30");
    assert_eq!(stored[0].duration_minutes, 30);
}

#[tokio::test]
async fn mapi_over_http_calendar_state_flags_cancel_is_hidden_for_existing_guarded_event() {
    let account = FakeStore::account();
    let event_id = Uuid::parse_str("d4d4d4d4-d4d4-d4d4-d4d4-d4d4d4d4d4d4").unwrap();
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
            owner_email: account.email.clone(),
            owner_display_name: account.display_name.clone(),
            rights: FakeStore::rights(),
            date: "2026-05-04".to_string(),
            time: "09:30".to_string(),
            time_zone: "UTC".to_string(),
            duration_minutes: 30,
            all_day: false,
            status: "confirmed".to_string(),
            sequence: 0,
            recurrence_rule: String::new(),
            recurrence_json: "{}".to_string(),
            recurrence_exceptions_json: "[]".to_string(),
            title: "State flags".to_string(),
            location: String::new(),
            organizer_json: "{}".to_string(),
            attendees: String::new(),
            attendees_json: String::new(),
            notes: String::new(),
            body_html: String::new(),
        }])),
        ..Default::default()
    };
    let events = store.events.clone();
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

    let mut property_values = Vec::new();
    append_mapi_i32_property(&mut property_values, 0x8217_0003, 0x5);

    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, test_mapi_folder_id(16));
    append_rop_open_message(
        &mut rops,
        1,
        2,
        test_mapi_folder_id(16),
        test_mapi_uuid_id(&event_id),
    );
    append_rop_set_properties(&mut rops, 2, 1, &property_values);

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
    assert!(contains_bytes(
        &response_rops,
        &0x8004_0102u32.to_le_bytes()
    ));
    let stored = events.lock().unwrap();
    assert_eq!(stored[0].status, "confirmed");
}

#[tokio::test]
async fn mapi_over_http_calendar_state_flags_reject_unsupported_bits_without_side_effect() {
    let account = FakeStore::account();
    let event_id = Uuid::parse_str("d6d6d6d6-d6d6-d6d6-d6d6-d6d6d6d6d6d6").unwrap();
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
            owner_email: account.email.clone(),
            owner_display_name: account.display_name.clone(),
            rights: FakeStore::rights(),
            date: "2026-05-04".to_string(),
            time: "09:30".to_string(),
            time_zone: "UTC".to_string(),
            duration_minutes: 30,
            all_day: false,
            status: "confirmed".to_string(),
            sequence: 0,
            recurrence_rule: String::new(),
            recurrence_json: "{}".to_string(),
            recurrence_exceptions_json: "[]".to_string(),
            title: "Unsupported state flags".to_string(),
            location: String::new(),
            organizer_json: "{}".to_string(),
            attendees: String::new(),
            attendees_json: String::new(),
            notes: String::new(),
            body_html: String::new(),
        }])),
        ..Default::default()
    };
    let events = store.events.clone();
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

    let mut property_values = Vec::new();
    append_mapi_i32_property(&mut property_values, 0x8217_0003, 0x8);

    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, test_mapi_folder_id(16));
    append_rop_open_message(
        &mut rops,
        1,
        2,
        test_mapi_folder_id(16),
        test_mapi_uuid_id(&event_id),
    );
    append_rop_set_properties(&mut rops, 2, 1, &property_values);

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
    assert!(contains_bytes(
        &response_rops,
        &0x8004_0102u32.to_le_bytes()
    ));
    let stored = events.lock().unwrap();
    assert_eq!(stored[0].status, "confirmed");
}

#[tokio::test]
async fn mapi_over_http_calendar_attachment_save_is_hidden_for_existing_guarded_event() {
    let account = FakeStore::account();
    let event_id = Uuid::parse_str("cccccccc-cccc-cccc-cccc-cccccccccccc").unwrap();
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
            owner_email: account.email.clone(),
            owner_display_name: account.display_name.clone(),
            rights: FakeStore::rights(),
            date: "2026-05-04".to_string(),
            time: "09:30".to_string(),
            time_zone: "UTC".to_string(),
            duration_minutes: 30,
            all_day: false,
            status: "confirmed".to_string(),
            sequence: 0,
            recurrence_rule: String::new(),
            recurrence_json: "{}".to_string(),
            recurrence_exceptions_json: "[]".to_string(),
            title: "Attached Calendar".to_string(),
            location: String::new(),
            organizer_json: "{}".to_string(),
            attendees: String::new(),
            attendees_json: String::new(),
            notes: String::new(),
            body_html: String::new(),
        }])),
        ..Default::default()
    };
    let calendar_attachments = store.calendar_attachments.clone();
    let service =
        ExchangeService::new_with_validator(store, Validator::new(FakeDetector::pdf(), 0.8));
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = HeaderValue::from_str(
        connect
            .headers()
            .get("set-cookie")
            .unwrap()
            .to_str()
            .unwrap()
            .split(';')
            .next()
            .unwrap(),
    )
    .unwrap();

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", cookie.clone());

    let mut attachment_properties = Vec::new();
    append_mapi_utf16_property(&mut attachment_properties, 0x3707_001F, "agenda.pdf");
    append_mapi_utf16_property(&mut attachment_properties, 0x370E_001F, "application/pdf");
    append_mapi_binary_property(&mut attachment_properties, 0x3701_0102, b"%PDF-calendar");
    let mut attachment_rops = Vec::new();
    append_rop_open_folder(&mut attachment_rops, 0, 1, test_mapi_folder_id(16));
    append_rop_open_message(
        &mut attachment_rops,
        1,
        2,
        test_mapi_folder_id(16),
        test_mapi_uuid_id(&event_id),
    );
    attachment_rops.extend_from_slice(&[
        0x23, 0x00, 0x02, 0x03, // RopCreateAttachment
        0x0A, 0x00, 0x03, // RopSetProperties
    ]);
    attachment_rops.extend_from_slice(&((attachment_properties.len() + 2) as u16).to_le_bytes());
    attachment_rops.extend_from_slice(&3u16.to_le_bytes());
    attachment_rops.extend_from_slice(&attachment_properties);
    attachment_rops.extend_from_slice(&[
        0x25, 0x00, 0x02, 0x03, 0x00, // RopSaveChangesAttachment
    ]);
    renew_mapi_request_id(&mut execute_headers);
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(
                &attachment_rops,
                &[1, u32::MAX, u32::MAX, u32::MAX],
            )),
        )
        .await
        .unwrap();
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(!contains_bytes(&response_rops, &[0x23, 0x03, 0, 0, 0, 0]));
    assert!(!contains_bytes(&response_rops, &[0x25, 0x02, 0, 0, 0, 0]));

    let stored = calendar_attachments.lock().unwrap();
    assert!(stored.get(&event_id).map_or(true, Vec::is_empty));
}

#[tokio::test]
async fn mapi_over_http_calendar_get_valid_attachments_projects_existing_event() {
    let account = FakeStore::account();
    let event_id = Uuid::parse_str("cdcdcdcd-cdcd-cdcd-cdcd-cdcdcdcdcdcd").unwrap();
    let first_attachment_id = Uuid::parse_str("adadadad-adad-adad-adad-adadadadadad").unwrap();
    let second_attachment_id = Uuid::parse_str("bebebebe-bebe-bebe-bebe-bebebebebebe").unwrap();
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
            owner_email: account.email.clone(),
            owner_display_name: account.display_name.clone(),
            rights: FakeStore::rights(),
            date: "2026-05-04".to_string(),
            time: "09:30".to_string(),
            time_zone: "UTC".to_string(),
            duration_minutes: 30,
            all_day: false,
            status: "confirmed".to_string(),
            sequence: 0,
            recurrence_rule: String::new(),
            recurrence_json: "{}".to_string(),
            recurrence_exceptions_json: "[]".to_string(),
            title: "Calendar valid attachments".to_string(),
            location: String::new(),
            organizer_json: "{}".to_string(),
            attendees: String::new(),
            attendees_json: String::new(),
            notes: String::new(),
            body_html: String::new(),
        }])),
        calendar_attachments: Arc::new(Mutex::new(HashMap::from([(
            event_id,
            vec![
                CalendarEventAttachment {
                    id: first_attachment_id,
                    event_id,
                    file_reference: format!("calendar-attachment:{event_id}:{first_attachment_id}"),
                    file_name: "first-agenda.pdf".to_string(),
                    media_type: "application/pdf".to_string(),
                    size_octets: 11,
                },
                CalendarEventAttachment {
                    id: second_attachment_id,
                    event_id,
                    file_reference: format!(
                        "calendar-attachment:{event_id}:{second_attachment_id}"
                    ),
                    file_name: "second-agenda.pdf".to_string(),
                    media_type: "application/pdf".to_string(),
                    size_octets: 22,
                },
            ],
        )]))),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = HeaderValue::from_str(
        connect
            .headers()
            .get("set-cookie")
            .unwrap()
            .to_str()
            .unwrap()
            .split(';')
            .next()
            .unwrap(),
    )
    .unwrap();

    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, test_mapi_folder_id(16));
    append_rop_open_message(
        &mut rops,
        1,
        2,
        test_mapi_folder_id(16),
        test_mapi_uuid_id(&event_id),
    );
    rops.extend_from_slice(&[
        0x52, 0x00, 0x02, // RopGetValidAttachments
    ]);

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", cookie);
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
    assert!(contains_bytes(
        &response_rops,
        &[0x52, 0x02, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 1, 0, 0, 0]
    ));
}

#[tokio::test]
async fn mapi_over_http_advertised_calendar_open_attachment_projects_existing_event() {
    let account = FakeStore::account();
    let event_id = Uuid::parse_str("cdcdcdcd-cdcd-cdcd-cdcd-cdcdcdcd0001").unwrap();
    let attachment_id = Uuid::parse_str("adadadad-adad-adad-adad-adadadad0001").unwrap();
    let store = FakeStore {
        session: Some(account.clone()),
        events: Arc::new(Mutex::new(vec![AccessibleEvent {
            id: event_id,
            uid: event_id.to_string(),
            collection_id: "default".to_string(),
            owner_account_id: account.account_id,
            owner_email: account.email.clone(),
            owner_display_name: account.display_name.clone(),
            rights: FakeStore::rights(),
            date: "2026-05-04".to_string(),
            time: "09:30".to_string(),
            time_zone: "UTC".to_string(),
            duration_minutes: 30,
            all_day: false,
            status: "confirmed".to_string(),
            sequence: 0,
            recurrence_rule: String::new(),
            recurrence_json: "{}".to_string(),
            recurrence_exceptions_json: "[]".to_string(),
            title: "Advertised Calendar attachment".to_string(),
            location: String::new(),
            organizer_json: "{}".to_string(),
            attendees: String::new(),
            attendees_json: String::new(),
            notes: String::new(),
            body_html: String::new(),
        }])),
        calendar_attachments: Arc::new(Mutex::new(HashMap::from([(
            event_id,
            vec![CalendarEventAttachment {
                id: attachment_id,
                event_id,
                file_reference: format!("calendar-attachment:{event_id}:{attachment_id}"),
                file_name: "default-agenda.pdf".to_string(),
                media_type: "application/pdf".to_string(),
                size_octets: 33,
            }],
        )]))),
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

    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, test_mapi_folder_id(16));
    append_rop_open_message(
        &mut rops,
        1,
        2,
        test_mapi_folder_id(16),
        test_mapi_uuid_id(&event_id),
    );
    rops.extend_from_slice(&[
        0x22, 0x00, 0x02, 0x03, 0x00, // RopOpenAttachment
    ]);
    rops.extend_from_slice(&0u32.to_le_bytes());
    append_rop_get_properties_specific(
        &mut rops,
        3,
        &[0x0E21_0003, 0x3707_001F, 0x370E_001F, 0x0E20_0003],
    );

    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(&response_rops, &[0x22, 0x03, 0, 0, 0, 0]));
    assert!(contains_bytes(
        &response_rops,
        &utf16z("default-agenda.pdf")
    ));
    assert!(contains_bytes(&response_rops, &utf16z("application/pdf")));
}

#[tokio::test]
async fn mapi_over_http_calendar_delete_attachment_is_hidden_for_existing_guarded_event() {
    let account = FakeStore::account();
    let event_id = Uuid::parse_str("dddddddd-dddd-dddd-dddd-dddddddddddd").unwrap();
    let attachment_id = Uuid::parse_str("afafafaf-afaf-afaf-afaf-afafafafafaf").unwrap();
    let calendar_attachments = Arc::new(Mutex::new(HashMap::from([(
        event_id,
        vec![CalendarEventAttachment {
            id: attachment_id,
            event_id,
            file_reference: format!("calendar-attachment:{event_id}:{attachment_id}"),
            file_name: "delete-agenda.pdf".to_string(),
            media_type: "application/pdf".to_string(),
            size_octets: 11,
        }],
    )])));
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
            owner_email: account.email.clone(),
            owner_display_name: account.display_name.clone(),
            rights: FakeStore::rights(),
            date: "2026-05-04".to_string(),
            time: "09:30".to_string(),
            time_zone: "UTC".to_string(),
            duration_minutes: 30,
            all_day: false,
            status: "confirmed".to_string(),
            sequence: 0,
            recurrence_rule: String::new(),
            recurrence_json: "{}".to_string(),
            recurrence_exceptions_json: "[]".to_string(),
            title: "Calendar delete attachment".to_string(),
            location: String::new(),
            organizer_json: "{}".to_string(),
            attendees: String::new(),
            attendees_json: String::new(),
            notes: String::new(),
            body_html: String::new(),
        }])),
        calendar_attachments: calendar_attachments.clone(),
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

    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, test_mapi_folder_id(16));
    append_rop_open_message(
        &mut rops,
        1,
        2,
        test_mapi_folder_id(16),
        test_mapi_uuid_id(&event_id),
    );
    rops.extend_from_slice(&[0x24, 0x00, 0x02]);
    rops.extend_from_slice(&0u32.to_le_bytes());

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
    assert!(!contains_bytes(&response_rops, &[0x24, 0x02, 0, 0, 0, 0]));
    assert_eq!(calendar_attachments.lock().unwrap()[&event_id].len(), 1);
}

#[tokio::test]
async fn mapi_over_http_task_crud_uses_canonical_tasks() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        task_collections: Arc::new(Mutex::new(vec![FakeStore::collection(
            "default", "tasks", "Tasks",
        )])),
        ..Default::default()
    };
    let tasks = store.tasks.clone();
    let deleted_tasks = store.deleted_tasks.clone();
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = HeaderValue::from_str(
        connect
            .headers()
            .get("set-cookie")
            .unwrap()
            .to_str()
            .unwrap()
            .split(';')
            .next()
            .unwrap(),
    )
    .unwrap();

    let mut property_values = Vec::new();
    append_mapi_utf16_property(&mut property_values, 0x0037_001F, "RCA Task");
    append_mapi_utf16_property(&mut property_values, 0x1000_001F, "Created through MAPI");
    let mut rops = Vec::new();
    append_rop_create_message(&mut rops, 0, 1, test_mapi_folder_id(19));
    append_rop_set_properties(&mut rops, 1, 2, &property_values);
    append_rop_save_changes_message(&mut rops, 1, 1);
    append_rop_get_properties_specific(&mut rops, 1, &[0x0037_001F, 0x1000_001F]);
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", cookie.clone());
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    let _response_rops = response_rops_from_execute_response(response).await;
    {
        let stored = tasks.lock().unwrap();
        assert_eq!(stored.len(), 1);
        assert_eq!(stored[0].title, "RCA Task");
        assert_eq!(stored[0].description, "Created through MAPI");
        assert_eq!(stored[0].status, "needs-action");
    }

    let task_id = Uuid::parse_str("eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee").unwrap();
    let mut update_values = Vec::new();
    append_mapi_utf16_property(&mut update_values, 0x0037_001F, "Updated RCA Task");
    append_mapi_i32_property(&mut update_values, 0x1090_0003, 1);
    let mut update_rops = Vec::new();
    append_rop_open_folder(&mut update_rops, 0, 1, test_mapi_folder_id(19));
    append_rop_open_message(
        &mut update_rops,
        1,
        2,
        test_mapi_folder_id(19),
        test_mapi_uuid_id(&task_id),
    );
    append_rop_set_properties(&mut update_rops, 2, 2, &update_values);
    renew_mapi_request_id(&mut execute_headers);
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&update_rops, &[1, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(!response_rops
        .windows(4)
        .any(|window| window == 0x8004_0102u32.to_le_bytes()));
    {
        let stored = tasks.lock().unwrap();
        assert_eq!(stored[0].title, "Updated RCA Task");
        assert_eq!(stored[0].status, "completed");
    }

    let mut delete_rops = Vec::new();
    append_rop_open_folder(&mut delete_rops, 0, 1, test_mapi_folder_id(19));
    append_rop_delete_messages(&mut delete_rops, 1, &[test_mapi_uuid_id(&task_id)]);
    renew_mapi_request_id(&mut execute_headers);
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&delete_rops, &[1, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(!response_rops
        .windows(4)
        .any(|window| window == 0x8004_0102u32.to_le_bytes()));
    assert!(tasks.lock().unwrap().is_empty());
    assert_eq!(deleted_tasks.lock().unwrap().as_slice(), &[task_id]);
}

#[tokio::test]
async fn mapi_over_http_task_contents_table_lists_canonical_tasks() {
    let task_list_id = "99999999-9999-9999-9999-999999999999";
    let task = FakeStore::task(
        "eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee",
        task_list_id,
        "Existing RCA Task",
    );
    let store = FakeStore {
        session: Some(FakeStore::account()),
        task_collections: Arc::new(Mutex::new(vec![FakeStore::collection(
            "default", "tasks", "Tasks",
        )])),
        tasks: Arc::new(Mutex::new(vec![task])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let mut cookie = mapi_cookie_header(&connect);
    let mut logon_rops = vec![0xFE, 0x00, 0x00, 0x01];
    let legacy_dn = format!(
        "/o=LPE/ou=Exchange Administrative Group/cn=Recipients/cn={}\0",
        FakeStore::account().email
    );
    logon_rops.extend_from_slice(&0x0100_0004u32.to_le_bytes());
    logon_rops.extend_from_slice(&0u32.to_le_bytes());
    logon_rops.extend_from_slice(&(legacy_dn.len() as u16).to_le_bytes());
    logon_rops.extend_from_slice(legacy_dn.as_bytes());
    let mut logon_headers = mapi_headers("Execute");
    logon_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let logon_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &logon_headers,
            &execute_body(&rop_buffer(&logon_rops, &[u32::MAX])),
        )
        .await
        .unwrap();
    assert_eq!(logon_response.status(), StatusCode::OK);
    cookie = mapi_cookie_header(&logon_response);

    let mut rops = vec![
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder
    ];
    append_mapi_wire_id(&mut rops, test_mapi_folder_id(19));
    rops.push(0);
    rops.extend_from_slice(&[
        0x05, 0x00, 0x01, 0x02, 0x00, // RopGetContentsTable
        0x12, 0x00, 0x02, 0x00, // RopSetColumns
    ]);
    rops.extend_from_slice(&2u16.to_le_bytes());
    rops.extend_from_slice(&0x0037_001Fu32.to_le_bytes());
    rops.extend_from_slice(&0x001A_001Fu32.to_le_bytes());
    rops.extend_from_slice(&[
        0x15, 0x00, 0x02, 0x00, 0x01, // RopQueryRows
    ]);
    rops.extend_from_slice(&10u16.to_le_bytes());

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
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
    assert!(contains_bytes(&response_rops, &utf16z("Existing RCA Task")));
    assert!(contains_bytes(&response_rops, &utf16z("IPM.Task")));
}

#[tokio::test]
async fn mapi_over_http_execute_opens_freebusy_data_folder() {
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

    let mut rops = vec![0x02, 0x00, 0x00, 0x01];
    append_mapi_wire_id(&mut rops, crate::mapi::identity::FREEBUSY_DATA_FOLDER_ID);
    rops.push(0);

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
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
    assert!(contains_bytes(&response_rops, &[0x02, 0x01, 0, 0, 0, 0]));
    assert!(!contains_bytes(
        &response_rops,
        &[0x02, 0x01, 0x0f, 0x01, 0x04, 0x80]
    ));
}

#[tokio::test]
async fn mapi_over_http_freebusy_data_folder_projects_local_freebusy_without_canonical_state() {
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

    let mut rops = Vec::new();
    append_rop_open_folder(
        &mut rops,
        0,
        1,
        crate::mapi::identity::FREEBUSY_DATA_FOLDER_ID,
    );
    rops.extend_from_slice(&[0x05, 0x00, 0x01, 0x02, 0x00]); // contents table
    rops.extend_from_slice(&[0x12, 0x00, 0x02, 0x00]);
    rops.extend_from_slice(&2u16.to_le_bytes());
    rops.extend_from_slice(&0x0037_001Fu32.to_le_bytes());
    rops.extend_from_slice(&0x001A_001Fu32.to_le_bytes());
    rops.extend_from_slice(&[0x15, 0x00, 0x02, 0x00, 0x01]);
    rops.extend_from_slice(&50u16.to_le_bytes());
    let mut restriction = vec![0x04, 0x04];
    restriction.extend_from_slice(&0x0037_001Fu32.to_le_bytes());
    restriction.extend_from_slice(&0x0037_001Fu32.to_le_bytes());
    restriction.extend_from_slice(&utf16z("LocalFreebusy"));
    rops.extend_from_slice(&[0x14, 0x00, 0x02, 0x00]); // RopRestrict
    rops.extend_from_slice(&(restriction.len() as u16).to_le_bytes());
    rops.extend_from_slice(&restriction);
    rops.extend_from_slice(&[0x17, 0x00, 0x02]); // RopQueryPosition

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
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
        contains_bytes(&response_rops, &[0x15, 0x02, 0, 0, 0, 0, 0x02, 1, 0]),
        "{response_rops:02x?}"
    );
    assert!(contains_bytes(&response_rops, &utf16z("LocalFreebusy")));
    assert!(!contains_bytes(
        &response_rops,
        &utf16z("IPM.Microsoft.Delegate")
    ));
    assert!(contains_bytes(
        &response_rops,
        &utf16z("IPM.Microsoft.ScheduleData.FreeBusy")
    ));
    assert!(
        contains_bytes(
            &response_rops,
            &[0x17, 0x02, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0]
        ),
        "{response_rops:02x?}"
    );
}

#[tokio::test]
async fn mapi_over_http_open_message_resolves_virtual_local_freebusy_without_folder_id() {
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

    let local_freebusy_id = crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFE4);
    let mut rops = Vec::new();
    append_rop_open_message(
        &mut rops,
        0,
        1,
        crate::mapi::identity::FREEBUSY_DATA_FOLDER_ID,
        local_freebusy_id,
    );
    rops[6..14].fill(0);
    append_rop_get_properties_specific(&mut rops, 1, &[0x0037_001F, 0x001A_001F]);

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
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
    assert!(!contains_bytes(
        &response_rops,
        &0x8004_010Fu32.to_le_bytes()
    ));
    assert!(
        contains_bytes(&response_rops, &utf16z("LocalFreebusy")),
        "{response_rops:02x?}"
    );
    assert!(
        contains_bytes(
            &response_rops,
            &utf16z("IPM.Microsoft.ScheduleData.FreeBusy")
        ),
        "{response_rops:02x?}"
    );
}

#[tokio::test]
async fn mapi_over_http_freebusy_data_folder_content_sync_projects_canonical_fai_messages() {
    let delegate_message_id = Uuid::parse_str("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee").unwrap();
    let freebusy_message_id = Uuid::parse_str("bbbbbbbb-cccc-dddd-eeee-ffffffffffff").unwrap();
    let store = FakeStore {
        session: Some(FakeStore::account()),
        delegate_freebusy_messages: Arc::new(Mutex::new(vec![
            DelegateFreeBusyMessageObject {
                id: delegate_message_id,
                account_id: Uuid::parse_str("11111111-1111-1111-1111-111111111111").unwrap(),
                owner_account_id: Uuid::parse_str("22222222-2222-2222-2222-222222222222").unwrap(),
                owner_email: "owner@example.test".to_string(),
                message_kind: "delegate".to_string(),
                subject: "Delegate access for owner@example.test".to_string(),
                body_text: "calendarRead=true; calendarWrite=true".to_string(),
                starts_at: None,
                ends_at: None,
                busy_status: None,
                payload_json: r#"{"canOpenCalendar":true}"#.to_string(),
                updated_at: "2026-01-01T00:00:00Z".to_string(),
            },
            DelegateFreeBusyMessageObject {
                id: freebusy_message_id,
                account_id: Uuid::parse_str("11111111-1111-1111-1111-111111111111").unwrap(),
                owner_account_id: Uuid::parse_str("22222222-2222-2222-2222-222222222222").unwrap(),
                owner_email: "owner@example.test".to_string(),
                message_kind: "freebusy".to_string(),
                subject: "Free/busy for owner@example.test".to_string(),
                body_text: "busy 2026-01-01T09:00:00Z/2026-01-01T10:00:00Z".to_string(),
                starts_at: Some("2026-01-01T09:00:00Z".to_string()),
                ends_at: Some("2026-01-01T10:00:00Z".to_string()),
                busy_status: Some("busy".to_string()),
                payload_json: r#"{"status":"busy"}"#.to_string(),
                updated_at: "2026-01-01T00:00:00Z".to_string(),
            },
        ])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = mapi_cookie_header(&connect);

    let mut rops = Vec::new();
    append_rop_open_folder(
        &mut rops,
        0,
        1,
        crate::mapi::identity::FREEBUSY_DATA_FOLDER_ID,
    );
    rops.extend_from_slice(&[
        0x70, 0x00, 0x01, 0x02, // RopSynchronizationConfigure
        0x01, 0x00, 0x10, 0x00, // content sync, FAI only
        0x00, 0x00, // RestrictionDataSize
        0x05, 0x00, 0x00, 0x00, // SynchronizationExtraFlags: Eid | CN
        0x00, 0x00, // PropertyTagCount
        0x4E, 0x00, 0x02, // RopFastTransferSourceGetBuffer
    ]);
    rops.extend_from_slice(&4096u16.to_le_bytes());

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
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
    assert_eq!(mapi_sync_manifest_counts(&response_rops), Some((0, 2)));
    let stream = strict_content_sync_transfer_from_response(&response_rops).unwrap();
    assert_eq!(stream.message_changes.len(), 2);
    assert!(stream
        .message_changes
        .iter()
        .all(|message| message.associated));
    assert!(stream
        .message_changes
        .iter()
        .any(|message| message.subject == "Delegate access for owner@example.test"));
    assert!(stream
        .message_changes
        .iter()
        .any(|message| message.subject == "Free/busy for owner@example.test"));
    assert!(contains_bytes(
        &response_rops,
        &utf16z("IPM.Microsoft.Delegate")
    ));
    assert!(contains_bytes(
        &response_rops,
        &utf16z("IPM.Microsoft.ScheduleData.FreeBusy")
    ));
}

#[tokio::test]
async fn mapi_over_http_ipm_subtree_hierarchy_findrow_finds_calendar_by_entry_id() {
    let account = FakeStore::account();
    let store = FakeStore {
        session: Some(account.clone()),
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
    let calendar_entry_id = crate::mapi::identity::folder_entry_id_from_object_id(
        account.account_id,
        crate::mapi::identity::CALENDAR_FOLDER_ID,
    )
    .unwrap();
    let mut restriction = Vec::new();
    append_search_property_binary(&mut restriction, 0x0FFF_0102, 0x04, &calendar_entry_id);

    let mut rops = Vec::new();
    append_rop_open_folder(
        &mut rops,
        0,
        1,
        crate::mapi::identity::IPM_SUBTREE_FOLDER_ID,
    );
    rops.extend_from_slice(&[
        0x04, 0x00, 0x01, 0x02, 0x04, // RopGetHierarchyTable
        0x12, 0x00, 0x02, 0x00, // RopSetColumns
    ]);
    rops.extend_from_slice(&3u16.to_le_bytes());
    rops.extend_from_slice(&0x3001_001Fu32.to_le_bytes());
    rops.extend_from_slice(&0x0FFF_0102u32.to_le_bytes());
    rops.extend_from_slice(&0x3613_001Fu32.to_le_bytes());
    rops.extend_from_slice(&[
        0x4F, 0x00, 0x02, 0x00, // RopFindRow
    ]);
    rops.extend_from_slice(&(restriction.len() as u16).to_le_bytes());
    rops.extend_from_slice(&restriction);
    rops.push(0);
    rops.extend_from_slice(&0u16.to_le_bytes());

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
    assert!(contains_bytes(
        &response_rops,
        &[0x4F, 0x02, 0, 0, 0, 0, 0, 1]
    ));
    assert!(contains_bytes(&response_rops, &utf16z("Calendar")));
    assert!(contains_bytes(&response_rops, &utf16z("IPF.Appointment")));
    assert!(contains_bytes(&response_rops, &calendar_entry_id));
}

#[tokio::test]
async fn mapi_over_http_calendar_default_entry_id_converts_to_openable_folder_id() {
    let account = FakeStore::account();
    let store = FakeStore {
        session: Some(account.clone()),
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
    let calendar_entry_id = crate::mapi::identity::folder_entry_id_from_object_id(
        account.account_id,
        crate::mapi::identity::CALENDAR_FOLDER_ID,
    )
    .unwrap();
    let embedded_long_term_id = &calendar_entry_id[22..46];

    let mut rops = vec![0xFE, 0x00, 0x01, 0x01]; // RopLogon, private mailbox.
    rops.extend_from_slice(&0u32.to_le_bytes());
    rops.extend_from_slice(&0u32.to_le_bytes());
    rops.extend_from_slice(&0u16.to_le_bytes());
    append_rop_get_properties_specific(&mut rops, 1, &[0x36D0_0102]);
    rops.extend_from_slice(&[0x44, 0x00, 0x01]); // RopIdFromLongTermId.
    rops.extend_from_slice(embedded_long_term_id);
    append_rop_open_folder(&mut rops, 1, 2, crate::mapi::identity::CALENDAR_FOLDER_ID);
    append_rop_get_properties_specific(&mut rops, 2, &[0x3001_001F, 0x3613_001F, 0x36E5_001F]);

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
    let calendar_entry_id_offset = response_rops
        .windows(calendar_entry_id.len())
        .position(|window| window == calendar_entry_id.as_slice())
        .expect("PidTagIpmAppointmentEntryId value missing");
    assert_eq!(
        crate::mapi::identity::object_id_from_folder_entry_id(
            &response_rops
                [calendar_entry_id_offset..calendar_entry_id_offset + calendar_entry_id.len()]
        ),
        Some(crate::mapi::identity::CALENDAR_FOLDER_ID)
    );
    let mut id_from_long_term_response = vec![0x44, 0x01, 0, 0, 0, 0];
    id_from_long_term_response.extend_from_slice(&mapi_wire_id_bytes(
        crate::mapi::identity::CALENDAR_FOLDER_ID,
    ));
    assert!(contains_bytes(&response_rops, &id_from_long_term_response));
    assert!(contains_bytes(
        &response_rops,
        &[0x02, 0x02, 0, 0, 0, 0, 0, 0]
    ));
    assert!(contains_bytes(&response_rops, &utf16z("Calendar")));
    assert!(contains_bytes(&response_rops, &utf16z("IPF.Appointment")));
    assert!(contains_bytes(&response_rops, &utf16z("IPM.Appointment")));
}

#[tokio::test]
async fn mapi_over_http_set_properties_updates_canonical_mail_reminder_state() {
    let message_id = "39393939-3939-3939-3939-393939393939";
    let inbox = FakeStore::mailbox("55555555-5555-5555-5555-555555555555", "inbox", "Inbox");
    let email = FakeStore::email(
        message_id,
        "55555555-5555-5555-5555-555555555555",
        "inbox",
        "Message reminder",
    );
    let emails = Arc::new(Mutex::new(vec![email]));
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
        emails: emails.clone(),
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

    let reminder_at = "2026-05-21T12:30:00Z";
    let mut property_values = Vec::new();
    property_values.extend_from_slice(&0x8503_000Bu32.to_le_bytes());
    property_values.push(1);
    append_mapi_i64_property(
        &mut property_values,
        0x8560_0040,
        mapi_mailstore::filetime_from_rfc3339_utc(reminder_at) as i64,
    );

    let mut rops = vec![
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder
    ];
    append_mapi_wire_id(&mut rops, test_mapi_folder_id(5));
    rops.push(0);
    rops.extend_from_slice(&[
        0x03, 0x00, 0x01, 0x02, // RopOpenMessage
    ]);
    rops.extend_from_slice(&0x0FFFu16.to_le_bytes());
    append_mapi_wire_id(&mut rops, test_mapi_folder_id(5));
    rops.push(0);
    append_mapi_wire_id(&mut rops, test_mapi_message_id(message_id));
    rops.extend_from_slice(&[
        0x0A, 0x00, 0x02, // RopSetProperties on opened message
    ]);
    rops.extend_from_slice(&((property_values.len() + 2) as u16).to_le_bytes());
    rops.extend_from_slice(&2u16.to_le_bytes());
    rops.extend_from_slice(&property_values);
    append_rop_save_changes_message(&mut rops, 2, 2);

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(
        &response_rops,
        &[0x0A, 0x02, 0, 0, 0, 0, 0, 0]
    ));
    assert!(contains_bytes(
        &response_rops,
        &[0x0C, 0x02, 0, 0, 0, 0, 0x02]
    ));
    let updated = emails.lock().unwrap()[0].clone();
    assert!(updated.reminder_set);
    assert_eq!(updated.reminder_at.as_deref(), Some(reminder_at));
}

#[tokio::test]
async fn mapi_over_http_virtual_calendar_content_sync_stores_virtual_checkpoint() {
    let account = FakeStore::account();
    let calendar = FakeStore::collection("default", "calendar", "Calendar");
    let event_id = Uuid::parse_str("71717171-7171-7171-7171-717171717171").unwrap();
    let store = FakeStore {
        session: Some(account.clone()),
        calendar_collections: Arc::new(Mutex::new(vec![calendar])),
        events: Arc::new(Mutex::new(vec![AccessibleEvent {
            id: event_id,
            uid: event_id.to_string(),
            collection_id: "default".to_string(),
            owner_account_id: account.account_id,
            owner_email: account.email,
            owner_display_name: account.display_name,
            rights: FakeStore::rights(),
            date: "2026-05-25".to_string(),
            time: "14:30".to_string(),
            time_zone: "UTC".to_string(),
            duration_minutes: 45,
            all_day: false,
            status: "confirmed".to_string(),
            sequence: 0,
            recurrence_rule: String::new(),
            recurrence_json: "{}".to_string(),
            recurrence_exceptions_json: "[]".to_string(),
            title: "Calendar sync appointment".to_string(),
            location: "Conference room".to_string(),
            organizer_json: "{}".to_string(),
            attendees: String::new(),
            attendees_json: String::new(),
            notes: "Calendar sync body".to_string(),
            body_html: String::new(),
        }])),
        ..Default::default()
    };
    *store.mapi_sync_changes.lock().unwrap() = MapiSyncChangeSet {
        current_change_sequence: 55,
        current_modseq: 41,
        ..Default::default()
    };

    let response_rops = content_sync_response_rops(store.clone(), 16, &[]).await;

    let stream = strict_content_sync_transfer_from_response(&response_rops).unwrap();
    assert_eq!(stream.message_changes.len(), 1);
    assert!(contains_bytes(&response_rops, &utf16z("IPM.Appointment")));
    assert!(contains_bytes(&response_rops, &utf16z("Conference room")));
    let checkpoint = store
        .fetch_mapi_sync_checkpoint(
            FakeStore::account().account_id,
            Some(
                mapi_mailstore::virtual_special_mailbox(crate::mapi::identity::CALENDAR_FOLDER_ID)
                    .unwrap()
                    .id,
            ),
            MapiCheckpointKind::Content,
        )
        .await
        .unwrap();
    let checkpoint = checkpoint.unwrap();
    assert_eq!(checkpoint.last_change_sequence, 55);
    assert_eq!(checkpoint.last_modseq, 41);
    assert_eq!(
        checkpoint
            .cursor_json
            .get("syncRootFolderId")
            .and_then(|id| id.as_u64()),
        Some(crate::mapi::identity::CALENDAR_FOLDER_ID)
    );
}

#[tokio::test]
async fn mapi_over_http_advertised_calendar_sync_projects_default_collection_event() {
    let account = FakeStore::account();
    let event_id = Uuid::parse_str("71717171-7171-7171-7171-717171710001").unwrap();
    let store = FakeStore {
        session: Some(account.clone()),
        events: Arc::new(Mutex::new(vec![AccessibleEvent {
            id: event_id,
            uid: event_id.to_string(),
            collection_id: "default".to_string(),
            owner_account_id: account.account_id,
            owner_email: account.email,
            owner_display_name: account.display_name,
            rights: FakeStore::rights(),
            date: "2026-06-02".to_string(),
            time: "11:00".to_string(),
            time_zone: "UTC".to_string(),
            duration_minutes: 30,
            all_day: false,
            status: "confirmed".to_string(),
            sequence: 0,
            recurrence_rule: String::new(),
            recurrence_json: "{}".to_string(),
            recurrence_exceptions_json: "[]".to_string(),
            title: "Advertised Calendar appointment".to_string(),
            location: "Room 16".to_string(),
            organizer_json: "{}".to_string(),
            attendees: String::new(),
            attendees_json: String::new(),
            notes: "Projected without collection row".to_string(),
            body_html: String::new(),
        }])),
        ..Default::default()
    };
    *store.mapi_sync_changes.lock().unwrap() = MapiSyncChangeSet {
        changed_calendar_event_ids: vec![event_id],
        current_change_sequence: 56,
        current_modseq: 42,
        ..Default::default()
    };

    let response_rops = content_sync_response_rops(store.clone(), 16, &[]).await;

    let stream = strict_content_sync_transfer_from_response(&response_rops).unwrap();
    assert_eq!(stream.message_changes.len(), 1);
    assert!(contains_bytes(&response_rops, &utf16z("IPM.Appointment")));
    assert!(contains_bytes(&response_rops, &utf16z("Room 16")));
    let checkpoint = store
        .fetch_mapi_sync_checkpoint(
            account.account_id,
            Some(
                mapi_mailstore::virtual_special_mailbox(crate::mapi::identity::CALENDAR_FOLDER_ID)
                    .unwrap()
                    .id,
            ),
            MapiCheckpointKind::Content,
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(checkpoint.last_change_sequence, 56);
    assert_eq!(checkpoint.last_modseq, 42);
}

#[tokio::test]
async fn mapi_over_http_advertised_calendar_open_message_projects_default_collection_event() {
    let account = FakeStore::account();
    let event_id = Uuid::parse_str("71717171-7171-7171-7171-717171710003").unwrap();
    let store = FakeStore {
        session: Some(account.clone()),
        events: Arc::new(Mutex::new(vec![AccessibleEvent {
            id: event_id,
            uid: event_id.to_string(),
            collection_id: "default".to_string(),
            owner_account_id: account.account_id,
            owner_email: account.email,
            owner_display_name: account.display_name,
            rights: FakeStore::rights(),
            date: "2026-06-02".to_string(),
            time: "13:00".to_string(),
            time_zone: "UTC".to_string(),
            duration_minutes: 45,
            all_day: false,
            status: "confirmed".to_string(),
            sequence: 0,
            recurrence_rule: String::new(),
            recurrence_json: "{}".to_string(),
            recurrence_exceptions_json: "[]".to_string(),
            title: "Advertised Calendar open appointment".to_string(),
            location: "Room 18".to_string(),
            organizer_json: r#"{"email":"alice@example.test","common_name":"Alice"}"#.to_string(),
            attendees: "Bob".to_string(),
            attendees_json: r#"{"attendees":[{"email":"bob@example.test","common_name":"Bob","role":"REQ-PARTICIPANT","partstat":"accepted","rsvp":false}]}"#.to_string(),
            notes: "Open without collection row".to_string(),
            body_html: "<p>Open without collection row</p>".to_string(),
        }])),
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

    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, test_mapi_folder_id(16));
    append_rop_open_message(
        &mut rops,
        1,
        2,
        test_mapi_folder_id(16),
        test_mapi_uuid_id(&event_id),
    );
    append_rop_get_properties_specific(
        &mut rops,
        2,
        &[
            0x001A_001F,
            0x0037_001F,
            0x3FFB_001F,
            0x1000_001F,
            0x1013_001F,
            0x0C1F_001F,
            0x0E04_001F,
            0x820D_0040,
            0x820E_0040,
        ],
    );
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    let response_rops = response_rops_from_execute_response(response).await;

    assert!(
        contains_bytes(&response_rops, &utf16z("IPM.Appointment")),
        "response rops: {response_rops:02x?}"
    );
    assert!(contains_bytes(
        &response_rops,
        &utf16z("Advertised Calendar open appointment")
    ));
    assert!(contains_bytes(&response_rops, &utf16z("Room 18")));
    assert!(contains_bytes(
        &response_rops,
        &utf16z("Open without collection row")
    ));
    assert!(contains_bytes(
        &response_rops,
        &utf16z("<p>Open without collection row</p>")
    ));
    assert!(contains_bytes(
        &response_rops,
        &utf16z("alice@example.test")
    ));
    assert!(contains_bytes(&response_rops, &utf16z("Bob")));
}

#[tokio::test]
async fn mapi_over_http_empty_virtual_calendar_sync_has_no_placeholder_rows() {
    let account = FakeStore::account();
    let store = FakeStore {
        session: Some(account.clone()),
        ..Default::default()
    };
    *store.mapi_sync_changes.lock().unwrap() = MapiSyncChangeSet {
        current_change_sequence: 9,
        current_modseq: 11,
        ..Default::default()
    };

    let response_rops = content_sync_response_rops(store.clone(), 16, &[]).await;

    let stream = strict_content_sync_transfer_from_response(&response_rops).unwrap();
    assert!(stream.message_changes.is_empty());
    assert!(!contains_bytes(
        &response_rops,
        &utf16z("IPM.Configuration.Calendar")
    ));
    assert!(!contains_bytes(&response_rops, &utf16z("IPM.Appointment")));
    let checkpoint = store
        .fetch_mapi_sync_checkpoint(
            account.account_id,
            Some(
                mapi_mailstore::virtual_special_mailbox(crate::mapi::identity::CALENDAR_FOLDER_ID)
                    .unwrap()
                    .id,
            ),
            MapiCheckpointKind::Content,
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(checkpoint.last_change_sequence, 9);
    assert_eq!(checkpoint.last_modseq, 11);
    assert_eq!(
        checkpoint
            .cursor_json
            .get("syncRootFolderId")
            .and_then(|id| id.as_u64()),
        Some(crate::mapi::identity::CALENDAR_FOLDER_ID)
    );
}

#[tokio::test]
async fn mapi_over_http_calendar_fai_only_sync_does_not_project_synthetic_configuration() {
    let account = FakeStore::account();
    let calendar = FakeStore::collection("default", "calendar", "Calendar");
    let store = FakeStore {
        session: Some(account.clone()),
        calendar_collections: Arc::new(Mutex::new(vec![calendar])),
        ..Default::default()
    };
    store
        .store_mapi_sync_checkpoint(
            account.account_id,
            Some(
                mapi_mailstore::virtual_special_mailbox(crate::mapi::identity::CALENDAR_FOLDER_ID)
                    .unwrap()
                    .id,
            ),
            MapiCheckpointKind::Content,
            4,
            5,
            serde_json::json!({
                "source": "emsmdb-ics-download",
                "syncRootFolderId": crate::mapi::identity::CALENDAR_FOLDER_ID
            }),
        )
        .await
        .unwrap();
    *store.mapi_sync_changes.lock().unwrap() = MapiSyncChangeSet {
        current_change_sequence: 4,
        current_modseq: 5,
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = mapi_cookie_header(&connect);

    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, crate::mapi::identity::CALENDAR_FOLDER_ID);
    rops.extend_from_slice(&[
        0x70, 0x00, 0x01, 0x02, // RopSynchronizationConfigure
        0x01, 0x00, 0x10, 0x00, // content sync, FAI only
        0x00, 0x00, // RestrictionDataSize
        0x0d, 0x00, 0x00, 0x00, // SynchronizationExtraFlags: Eid | MessageSize | CN
        0x00, 0x00, // PropertyTagCount
        0x75, 0x00, 0x02, // RopSynchronizationUploadStateStreamBegin
    ]);
    rops.extend_from_slice(&0x4017_0102u32.to_le_bytes());
    rops.extend_from_slice(&0u32.to_le_bytes());
    rops.extend_from_slice(&[
        0x77, 0x00, 0x02, // RopSynchronizationUploadStateStreamEnd
        0x75, 0x00, 0x02, // RopSynchronizationUploadStateStreamBegin
    ]);
    rops.extend_from_slice(&0x6796_0102u32.to_le_bytes());
    rops.extend_from_slice(&0u32.to_le_bytes());
    rops.extend_from_slice(&[
        0x77, 0x00, 0x02, // RopSynchronizationUploadStateStreamEnd
        0x75, 0x00, 0x02, // RopSynchronizationUploadStateStreamBegin
    ]);
    rops.extend_from_slice(&0x67DA_0102u32.to_le_bytes());
    rops.extend_from_slice(&0u32.to_le_bytes());
    rops.extend_from_slice(&[
        0x77, 0x00, 0x02, // RopSynchronizationUploadStateStreamEnd
        0x75, 0x00, 0x02, // RopSynchronizationUploadStateStreamBegin
    ]);
    rops.extend_from_slice(&0x67D2_0102u32.to_le_bytes());
    rops.extend_from_slice(&0u32.to_le_bytes());
    rops.extend_from_slice(&[
        0x77, 0x00, 0x02, // RopSynchronizationUploadStateStreamEnd
        0x4E, 0x00, 0x02, // RopFastTransferSourceGetBuffer
    ]);
    rops.extend_from_slice(&4096u16.to_le_bytes());

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
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

    let stream = strict_content_sync_transfer_from_response(&response_rops).unwrap();
    assert!(stream.message_changes.is_empty());
    assert!(!contains_bytes(
        &response_rops,
        &utf16z("IPM.Configuration.Calendar")
    ));
    assert!(!contains_bytes(&response_rops, b"<UserConfiguration>"));
    assert!(!contains_bytes(&response_rops, b"18-OLPrefsVersion"));
    assert!(!contains_bytes(
        &response_rops,
        &utf16z("IPM.Configuration.CategoryList")
    ));
    assert!(!contains_bytes(
        &response_rops,
        &utf16z("IPM.Configuration.WorkHours")
    ));
    assert!(!contains_bytes(&response_rops, b"CategoryList.xsd"));
    assert!(!contains_bytes(&response_rops, b"WorkingHours.xsd"));
}

#[tokio::test]
async fn mapi_over_http_calendar_associated_contents_columns_include_configuration_properties() {
    let account = FakeStore::account();
    let calendar = FakeStore::collection("default", "calendar", "Calendar");
    let store = FakeStore {
        session: Some(account),
        calendar_collections: Arc::new(Mutex::new(vec![calendar])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = mapi_cookie_header(&connect);

    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, crate::mapi::identity::CALENDAR_FOLDER_ID);
    rops.extend_from_slice(&[
        0x05, 0x00, 0x01, 0x02, 0x02, // RopGetContentsTable, associated contents
        0x37, 0x00, 0x02, // RopQueryColumnsAll
    ]);

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
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

    let query_columns_offset = 18;
    assert_eq!(response_rops[query_columns_offset], 0x37);
    assert!(contains_bytes(
        &response_rops,
        &0x67AA_000Bu32.to_le_bytes()
    ));
    assert!(contains_bytes(
        &response_rops,
        &0x7C06_0003u32.to_le_bytes()
    ));
    assert!(contains_bytes(
        &response_rops,
        &0x7C07_0102u32.to_le_bytes()
    ));
    assert!(contains_bytes(
        &response_rops,
        &0x7C08_0102u32.to_le_bytes()
    ));
}

#[tokio::test]
async fn mapi_over_http_calendar_sync_hides_postgresql_canonical_event_properties(
) -> anyhow::Result<()> {
    let Some(fixture) = postgres_mapi_calendar_fixture().await? else {
        return Ok(());
    };
    let storage = fixture.storage.clone();
    let account_id = fixture.account_id;
    let event_id = Uuid::parse_str("71717171-7171-4171-9171-717171717171").unwrap();
    storage
        .upsert_client_event(UpsertClientEventInput {
            id: Some(event_id),
            account_id,
            uid: "mapi-calendar-postgres".to_string(),
            date: "2026-05-25".to_string(),
            time: "14:30".to_string(),
            time_zone: "UTC".to_string(),
            duration_minutes: 0,
            all_day: true,
            status: "tentative".to_string(),
            sequence: 7,
            recurrence_rule: "FREQ=DAILY;COUNT=2".to_string(),
            recurrence_json: r#"{"frequency":"daily","count":2}"#.to_string(),
            recurrence_exceptions_json: "[]".to_string(),
            title: "PostgreSQL calendar appointment".to_string(),
            location: "Room 420".to_string(),
            organizer_json: r#"{"email":"alice@example.test","common_name":"Alice Calendar"}"#.to_string(),
            attendees: "bob@example.test".to_string(),
            attendees_json: r#"{"organizer":{"email":"alice@example.test","common_name":"Alice Calendar"},"attendees":[{"email":"bob@example.test","common_name":"Bob","role":"REQ-PARTICIPANT","partstat":"accepted","rsvp":false}]}"#.to_string(),
            notes: "Canonical body text".to_string(),
            body_html: "<p>Canonical body text</p>".to_string(),
        })
        .await?;
    storage
        .add_calendar_event_attachment(
            account_id,
            event_id,
            AttachmentUploadInput {
                file_name: "agenda.pdf".to_string(),
                media_type: "application/pdf".to_string(),
                content_id: None,
                disposition: Some("attachment".to_string()),
                blob_bytes: b"calendar attachment".to_vec(),
            },
            lpe_storage::AuditEntryInput {
                actor: "alice@example.test".to_string(),
                action: "test-calendar-sync-attachment".to_string(),
                subject: event_id.to_string(),
            },
        )
        .await?;

    let response_rops = content_sync_response_rops_for_store(
        storage,
        crate::mapi::identity::CALENDAR_FOLDER_ID,
        &[],
    )
    .await;
    let stream = strict_content_sync_transfer_from_response(&response_rops).unwrap();
    assert!(stream.message_changes.is_empty());
    assert!(!contains_bytes(&response_rops, &utf16z("IPM.Appointment")));
    assert!(!contains_bytes(&response_rops, &utf16z("Room 420")));
    assert!(!contains_bytes(
        &response_rops,
        &utf16z("Canonical body text")
    ));

    fixture.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn mapi_over_http_calendar_contents_table_projects_postgresql_canonical_event_properties(
) -> anyhow::Result<()> {
    let Some(fixture) = postgres_mapi_calendar_fixture().await? else {
        return Ok(());
    };
    let storage = fixture.storage.clone();
    storage
        .upsert_client_event(UpsertClientEventInput {
            id: Some(Uuid::parse_str("72727272-7272-4272-9272-727272727272").unwrap()),
            account_id: fixture.account_id,
            uid: "mapi-calendar-table-postgres".to_string(),
            date: "2026-05-25".to_string(),
            time: "09:00".to_string(),
            time_zone: "UTC".to_string(),
            duration_minutes: 45,
            all_day: false,
            status: "confirmed".to_string(),
            sequence: 1,
            recurrence_rule: String::new(),
            recurrence_json: "{}".to_string(),
            recurrence_exceptions_json: "[]".to_string(),
            title: "Contents table appointment".to_string(),
            location: "Room 421".to_string(),
            organizer_json: r#"{"email":"alice@example.test","common_name":"Alice Calendar"}"#
                .to_string(),
            attendees: String::new(),
            attendees_json: r#"{"organizer":{"email":"alice@example.test","common_name":"Alice Calendar"},"attendees":[{"email":"bob@example.test","common_name":"Bob","role":"REQ-PARTICIPANT","partstat":"accepted","rsvp":false}]}"#.to_string(),
            notes: "Contents table body".to_string(),
            body_html: "<p>Contents table body</p>".to_string(),
        })
        .await?;

    let service = ExchangeService::new(storage);
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
    append_rop_open_folder(&mut rops, 0, 1, crate::mapi::identity::CALENDAR_FOLDER_ID);
    rops.extend_from_slice(&[
        0x05, 0x00, 0x01, 0x02, 0x00, // RopGetContentsTable
        0x12, 0x00, 0x02, 0x00, // RopSetColumns
    ]);
    let columns = [
        0x001A_001Fu32, // PidTagMessageClass
        0x0037_001Fu32, // PidTagSubject
        0x1000_001Fu32, // PidTagBody
        0x1013_001Fu32, // PidTagBodyHtml
        0x0C1A_001Fu32, // PidTagSenderName
        0x0C1F_001Fu32, // PidTagSenderEmailAddress
        0x0E04_001Fu32, // PidTagDisplayTo
        0x8238_001Fu32, // PidLidAllAttendeesString
        0x823B_001Fu32, // PidLidToAttendeesString
        0x823C_001Fu32, // PidLidCcAttendeesString
        0x820D_0040u32, // PidLidAppointmentStartWhole
        0x820E_0040u32, // PidLidAppointmentEndWhole
        0x8205_0003u32, // PidLidBusyStatus
        0x8215_000Bu32, // PidLidAppointmentSubType
        0x8217_0003u32, // PidLidAppointmentStateFlags
        0x8001_0102u32, // PidLidGlobalObjectId
        0x8002_0102u32, // PidLidCleanGlobalObjectId
        0x3FFB_001Fu32, // PidTagLocation
        0x0E1B_000Bu32, // PidTagHasAttachments
    ];
    rops.extend_from_slice(&(columns.len() as u16).to_le_bytes());
    for column in columns {
        rops.extend_from_slice(&column.to_le_bytes());
    }
    rops.extend_from_slice(&[
        0x15, 0x00, 0x02, 0x00, 0x01, // RopQueryRows
    ]);
    rops.extend_from_slice(&1u16.to_le_bytes());
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
    assert!(contains_bytes(&response_rops, &utf16z("IPM.Appointment")));
    assert!(contains_bytes(
        &response_rops,
        &utf16z("Contents table appointment")
    ));
    assert!(contains_bytes(
        &response_rops,
        &utf16z("Contents table body")
    ));
    assert!(contains_bytes(
        &response_rops,
        &utf16z("<p>Contents table body</p>")
    ));
    assert!(contains_bytes(&response_rops, &utf16z("Alice Calendar")));
    assert!(contains_bytes(
        &response_rops,
        &utf16z("alice@example.test")
    ));
    assert!(contains_bytes(&response_rops, &utf16z("Bob")));
    assert!(contains_bytes(&response_rops, &utf16z("Room 421")));

    fixture.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn mapi_over_http_calendar_sync_hides_postgresql_custom_calendar_collection(
) -> anyhow::Result<()> {
    let Some(fixture) = postgres_mapi_calendar_fixture().await? else {
        return Ok(());
    };
    let storage = fixture.storage.clone();
    let collection = storage
        .create_accessible_calendar_collection(fixture.account_id, "Outlook Custom Calendar")
        .await?;
    let event = storage
        .create_accessible_event(
            fixture.account_id,
            Some(&collection.id),
            UpsertClientEventInput {
                id: Some(Uuid::parse_str("73737373-7373-4373-9373-737373737373").unwrap()),
                account_id: fixture.account_id,
                uid: "mapi-calendar-custom-postgres".to_string(),
                date: "2026-05-25".to_string(),
                time: "11:00".to_string(),
                time_zone: "UTC".to_string(),
                duration_minutes: 30,
                all_day: false,
                status: "confirmed".to_string(),
                sequence: 1,
                recurrence_rule: String::new(),
                recurrence_json: "{}".to_string(),
                recurrence_exceptions_json: "[]".to_string(),
                title: "Custom calendar appointment".to_string(),
                location: "Room 422".to_string(),
                organizer_json: "{}".to_string(),
                attendees: String::new(),
                attendees_json: "{}".to_string(),
                notes: "Custom calendar body".to_string(),
                body_html: String::new(),
            },
        )
        .await?;
    assert_eq!(event.collection_id, collection.id);
    let canonical_events = storage
        .fetch_accessible_events_in_collection(fixture.account_id, &collection.id)
        .await?;
    assert_eq!(canonical_events.len(), 1);

    let snapshot = storage
        .load_mapi_mail_store(fixture.account_id, 500)
        .await?;
    let folder = snapshot
        .collaboration_folders()
        .iter()
        .find(|folder| folder.collection.display_name == "Outlook Custom Calendar")
        .expect("custom calendar folder projected");
    assert_ne!(folder.id, crate::mapi::identity::CALENDAR_FOLDER_ID);
    assert_eq!(snapshot.events_for_folder(folder.id).len(), 1);

    let response_rops = content_sync_response_rops_for_store(storage, folder.id, &[]).await;
    let stream = strict_content_sync_transfer_from_response(&response_rops).unwrap();
    assert!(stream.message_changes.is_empty());
    assert!(!contains_bytes(&response_rops, &utf16z("IPM.Appointment")));
    assert!(!contains_bytes(&response_rops, &utf16z("Room 422")));
    assert!(!contains_bytes(
        &response_rops,
        &utf16z("Custom calendar body")
    ));

    fixture.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn mapi_over_http_calendar_create_uses_postgresql_custom_calendar_collection(
) -> anyhow::Result<()> {
    let Some(fixture) = postgres_mapi_calendar_fixture().await? else {
        return Ok(());
    };
    let storage = fixture.storage.clone();
    let collection = storage
        .create_accessible_calendar_collection(fixture.account_id, "Outlook Custom Calendar")
        .await?;
    let snapshot = storage
        .load_mapi_mail_store(fixture.account_id, 500)
        .await?;
    let folder = snapshot
        .collaboration_folders()
        .iter()
        .find(|folder| folder.collection.id == collection.id)
        .expect("custom calendar folder projected");
    assert_ne!(folder.id, crate::mapi::identity::CALENDAR_FOLDER_ID);

    let service = ExchangeService::new(storage.clone());
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert(
        "cookie",
        HeaderValue::from_str(&mapi_cookie_header(&connect)).unwrap(),
    );
    let mut property_values = Vec::new();
    append_mapi_utf16_property(
        &mut property_values,
        0x0037_001F,
        "Created in custom calendar",
    );
    append_mapi_i64_property(
        &mut property_values,
        0x0060_0040,
        test_filetime("2026-06-06", "14:00"),
    );
    append_mapi_i64_property(
        &mut property_values,
        0x0061_0040,
        test_filetime("2026-06-06", "15:30"),
    );
    append_mapi_utf16_property(&mut property_values, 0x3FFB_001F, "Room 700");

    let mut rops = Vec::new();
    append_rop_create_message(&mut rops, 0, 1, folder.id);
    append_rop_set_properties(&mut rops, 1, 4, &property_values);
    append_rop_save_changes_message(&mut rops, 1, 1);
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
        !response_rops
            .windows(4)
            .any(|window| window == 0x8004_0102u32.to_le_bytes())
            && !response_rops
                .windows(4)
                .any(|window| window == 0x8004_010Fu32.to_le_bytes()),
        "custom calendar create returned an error: {response_rops:02x?}"
    );

    let custom_events = storage
        .fetch_accessible_events_in_collection(fixture.account_id, &collection.id)
        .await?;
    assert_eq!(custom_events.len(), 1);
    assert_eq!(custom_events[0].collection_id, collection.id);
    assert_eq!(custom_events[0].title, "Created in custom calendar");
    assert_eq!(custom_events[0].date, "2026-06-06");
    assert_eq!(custom_events[0].time, "14:00");
    assert_eq!(custom_events[0].duration_minutes, 90);
    assert_eq!(custom_events[0].location, "Room 700");
    let default_events = storage
        .fetch_accessible_events_in_collection(fixture.account_id, "default")
        .await?;
    assert!(default_events.is_empty());

    fixture.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn mapi_over_http_calendar_reopen_update_is_hidden_for_postgresql_custom_calendar_collection(
) -> anyhow::Result<()> {
    let Some(fixture) = postgres_mapi_calendar_fixture().await? else {
        return Ok(());
    };
    let storage = fixture.storage.clone();
    let collection = storage
        .create_accessible_calendar_collection(fixture.account_id, "Outlook Custom Calendar")
        .await?;
    let event = storage
        .create_accessible_event(
            fixture.account_id,
            Some(&collection.id),
            UpsertClientEventInput {
                id: Some(Uuid::parse_str("74747474-7474-4474-9474-747474747474").unwrap()),
                account_id: fixture.account_id,
                uid: "mapi-calendar-custom-update-postgres".to_string(),
                date: "2026-06-07".to_string(),
                time: "09:00".to_string(),
                time_zone: "UTC".to_string(),
                duration_minutes: 30,
                all_day: false,
                status: "confirmed".to_string(),
                sequence: 0,
                recurrence_rule: String::new(),
                recurrence_json: "{}".to_string(),
                recurrence_exceptions_json: "[]".to_string(),
                title: "Custom calendar before update".to_string(),
                location: "Room 701".to_string(),
                organizer_json: "{}".to_string(),
                attendees: String::new(),
                attendees_json: "{}".to_string(),
                notes: "Before update".to_string(),
                body_html: String::new(),
            },
        )
        .await?;
    let snapshot = storage
        .load_mapi_mail_store(fixture.account_id, 500)
        .await?;
    let folder = snapshot
        .collaboration_folders()
        .iter()
        .find(|folder| folder.collection.id == collection.id)
        .expect("custom calendar folder projected");
    let mapi_event = snapshot
        .events_for_folder(folder.id)
        .into_iter()
        .find(|candidate| candidate.canonical_id == event.id)
        .expect("custom calendar event projected");

    let service = ExchangeService::new(storage.clone());
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert(
        "cookie",
        HeaderValue::from_str(&mapi_cookie_header(&connect)).unwrap(),
    );
    let mut property_values = Vec::new();
    append_mapi_utf16_property(
        &mut property_values,
        0x0037_001F,
        "Custom calendar after update",
    );
    append_mapi_i64_property(
        &mut property_values,
        0x0060_0040,
        test_filetime("2026-06-07", "10:15"),
    );
    append_mapi_i64_property(
        &mut property_values,
        0x0061_0040,
        test_filetime("2026-06-07", "11:00"),
    );
    append_mapi_utf16_property(&mut property_values, 0x3FFB_001F, "Room 702");
    append_mapi_utf16_property(&mut property_values, 0x1000_001F, "After update");

    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, folder.id);
    append_rop_open_message(&mut rops, 1, 2, folder.id, mapi_event.id);
    append_rop_set_properties(&mut rops, 2, 5, &property_values);
    append_rop_save_changes_message(&mut rops, 2, 2);
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[1, 2, u32::MAX])),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(
        response_rops
            .windows(4)
            .any(|window| window == 0x8004_0102u32.to_le_bytes())
            || response_rops
                .windows(4)
                .any(|window| window == 0x8004_010Fu32.to_le_bytes()),
        "custom calendar update should be hidden: {response_rops:02x?}"
    );

    let custom_events = storage
        .fetch_accessible_events_in_collection(fixture.account_id, &collection.id)
        .await?;
    assert_eq!(custom_events.len(), 1);
    assert_eq!(custom_events[0].id, event.id);
    assert_eq!(custom_events[0].collection_id, collection.id);
    assert_eq!(custom_events[0].title, "Custom calendar before update");
    assert_eq!(custom_events[0].date, "2026-06-07");
    assert_eq!(custom_events[0].time, "09:00");
    assert_eq!(custom_events[0].duration_minutes, 30);
    assert_eq!(custom_events[0].location, "Room 701");
    assert_eq!(custom_events[0].notes, "Before update");
    let default_events = storage
        .fetch_accessible_events_in_collection(fixture.account_id, "default")
        .await?;
    assert!(default_events.is_empty());

    fixture.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn mapi_over_http_calendar_custom_collection_attachment_is_hidden_for_existing_guarded_event(
) -> anyhow::Result<()> {
    let Some(fixture) = postgres_mapi_calendar_fixture().await? else {
        return Ok(());
    };
    let storage = fixture.storage.clone();
    let collection = storage
        .create_accessible_calendar_collection(fixture.account_id, "Outlook Custom Calendar")
        .await?;
    let event = storage
        .create_accessible_event(
            fixture.account_id,
            Some(&collection.id),
            UpsertClientEventInput {
                id: Some(Uuid::parse_str("75757575-7575-4575-9575-757575757575").unwrap()),
                account_id: fixture.account_id,
                uid: "mapi-calendar-custom-attachment-postgres".to_string(),
                date: "2026-06-08".to_string(),
                time: "09:00".to_string(),
                time_zone: "UTC".to_string(),
                duration_minutes: 30,
                all_day: false,
                status: "confirmed".to_string(),
                sequence: 0,
                recurrence_rule: String::new(),
                recurrence_json: "{}".to_string(),
                recurrence_exceptions_json: "[]".to_string(),
                title: "Custom calendar attachment".to_string(),
                location: "Room 703".to_string(),
                organizer_json: "{}".to_string(),
                attendees: String::new(),
                attendees_json: "{}".to_string(),
                notes: String::new(),
                body_html: String::new(),
            },
        )
        .await?;
    let snapshot = storage
        .load_mapi_mail_store(fixture.account_id, 500)
        .await?;
    let folder = snapshot
        .collaboration_folders()
        .iter()
        .find(|folder| folder.collection.id == collection.id)
        .expect("custom calendar folder projected");
    let mapi_event = snapshot
        .events_for_folder(folder.id)
        .into_iter()
        .find(|candidate| candidate.canonical_id == event.id)
        .expect("custom calendar event projected");

    let service = ExchangeService::new_with_validator(
        storage.clone(),
        Validator::new(FakeDetector::pdf(), 0.8),
    );
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert(
        "cookie",
        HeaderValue::from_str(&mapi_cookie_header(&connect)).unwrap(),
    );
    let mut attachment_properties = Vec::new();
    append_mapi_utf16_property(&mut attachment_properties, 0x3707_001F, "custom-agenda.pdf");
    append_mapi_utf16_property(&mut attachment_properties, 0x370E_001F, "application/pdf");
    append_mapi_binary_property(
        &mut attachment_properties,
        0x3701_0102,
        b"%PDF-custom-calendar",
    );

    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, folder.id);
    append_rop_open_message(&mut rops, 1, 2, folder.id, mapi_event.id);
    rops.extend_from_slice(&[
        0x23, 0x00, 0x02, 0x03, // RopCreateAttachment
        0x0A, 0x00, 0x03, // RopSetProperties
    ]);
    rops.extend_from_slice(&((attachment_properties.len() + 2) as u16).to_le_bytes());
    rops.extend_from_slice(&3u16.to_le_bytes());
    rops.extend_from_slice(&attachment_properties);
    rops.extend_from_slice(&[
        0x25, 0x00, 0x02, 0x03, 0x00, // RopSaveChangesAttachment
    ]);
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[1, 2, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(!contains_bytes(&response_rops, &[0x23, 0x03, 0, 0, 0, 0]));
    assert!(!contains_bytes(&response_rops, &[0x25, 0x02, 0, 0, 0, 0]));

    let attachments = storage
        .fetch_calendar_event_attachments(fixture.account_id, event.id)
        .await?;
    assert!(attachments.is_empty());
    let default_events = storage
        .fetch_accessible_events_in_collection(fixture.account_id, "default")
        .await?;
    assert!(default_events.is_empty());

    let snapshot = storage
        .load_mapi_mail_store(fixture.account_id, 500)
        .await?;
    let folder = snapshot
        .collaboration_folders()
        .iter()
        .find(|folder| folder.collection.id == collection.id)
        .expect("custom calendar folder projected after attachment save");
    let mapi_event = snapshot
        .events_for_folder(folder.id)
        .into_iter()
        .find(|candidate| candidate.canonical_id == event.id)
        .expect("custom calendar event projected after attachment save");

    let reopen_service = ExchangeService::new(storage.clone());
    let connect = reopen_service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert(
        "cookie",
        HeaderValue::from_str(&mapi_cookie_header(&connect)).unwrap(),
    );
    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, folder.id);
    append_rop_open_message(&mut rops, 1, 2, folder.id, mapi_event.id);
    rops.extend_from_slice(&[
        0x21, 0x00, 0x02, 0x03, 0x00, // RopGetAttachmentTable
        0x22, 0x00, 0x02, 0x04, 0x00, // RopOpenAttachment
    ]);
    rops.extend_from_slice(&0u32.to_le_bytes());
    append_rop_get_properties_specific(
        &mut rops,
        4,
        &[0x0E21_0003, 0x3707_001F, 0x370E_001F, 0x0E20_0003],
    );
    let response = reopen_service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[1, 2, u32::MAX, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(!contains_bytes(
        &response_rops,
        &[0x21, 0x03, 0, 0, 0, 0, 1, 0, 0, 0]
    ));
    assert!(!contains_bytes(&response_rops, &[0x22, 0x04, 0, 0, 0, 0]));
    assert!(!contains_bytes(
        &response_rops,
        &utf16z("custom-agenda.pdf")
    ));
    assert!(!contains_bytes(&response_rops, &utf16z("application/pdf")));
    assert!(!contains_bytes(
        &response_rops,
        &(b"%PDF-custom-calendar".len() as u32).to_le_bytes()
    ));

    fixture.cleanup().await?;
    Ok(())
}

#[test]
fn mapi_hierarchy_sync_keeps_direct_reminders_projection_out_of_normal_hierarchy() {
    let reminders =
        mapi_mailstore::virtual_special_mailbox(crate::mapi::identity::REMINDERS_FOLDER_ID)
            .expect("Reminders mailbox");
    let buffer = mapi_mailstore::sync_manifest_buffer_with_final_state(
        Uuid::nil(),
        0x02,
        0x0101,
        0,
        &[
            PID_TAG_FOLDER_TYPE,
            PID_TAG_CONTENT_COUNT,
            PID_TAG_CONTENT_UNREAD_COUNT,
            0x0E08_0003,
            0x0FF4_0003,
            0x3FE0_0102,
            0x3FE1_0102,
            0x0E27_0102,
        ],
        crate::mapi::identity::IPM_SUBTREE_FOLDER_ID,
        std::slice::from_ref(&reminders),
        &[],
        &[],
        &[],
        std::slice::from_ref(&reminders),
        std::slice::from_ref(&reminders),
        &[],
        &[],
        &[],
        &[],
        1,
    );
    let decoded = strict_decode_hierarchy_sync_stream(&buffer).expect("strict hierarchy ICS");
    assert_eq!(decoded.folder_changes.len(), 1);
    assert_eq!(decoded.folder_changes[0].display_name, "Reminders");
    assert_eq!(decoded.folder_changes[0].folder_type, None);
    assert_eq!(
        decoded.folder_changes[0].parent_source_key,
        Vec::<u8>::new()
    );
    assert_eq!(
        decoded.folder_changes[0].parent_folder_id,
        Some(crate::mapi::identity::ROOT_FOLDER_ID)
    );
    assert!(contains_bytes(&buffer, &utf16z("Outlook.Reminder")));
}

#[tokio::test]
async fn mapi_over_http_hierarchy_inbox_default_calendar_entry_id_uses_account_guid() {
    let account = FakeStore::account();
    let store = FakeStore {
        session: Some(account.clone()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            "55555555-5555-5555-5555-555555555555",
            "inbox",
            "Inbox",
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

    let mut rops = Vec::new();
    append_rop_open_folder(
        &mut rops,
        0,
        1,
        crate::mapi::identity::IPM_SUBTREE_FOLDER_ID,
    );
    rops.extend_from_slice(&[
        0x04, 0x00, 0x01, 0x02, 0x04, // RopGetHierarchyTable
        0x12, 0x00, 0x02, 0x00, // RopSetColumns
    ]);
    rops.extend_from_slice(&2u16.to_le_bytes());
    rops.extend_from_slice(&0x3001_001Fu32.to_le_bytes());
    rops.extend_from_slice(&0x36D0_0102u32.to_le_bytes());
    rops.extend_from_slice(&[
        0x15, 0x00, 0x02, 0x00, 0x01, // RopQueryRows
    ]);
    rops.extend_from_slice(&50u16.to_le_bytes());

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
    let account_calendar_entry_id = crate::mapi::identity::folder_entry_id_from_object_id(
        account.account_id,
        crate::mapi::identity::CALENDAR_FOLDER_ID,
    )
    .unwrap();
    let nil_calendar_entry_id = crate::mapi::identity::folder_entry_id_from_object_id(
        Uuid::nil(),
        crate::mapi::identity::CALENDAR_FOLDER_ID,
    )
    .unwrap();
    assert!(contains_bytes(&response_rops, &utf16z("Inbox")));
    assert!(contains_bytes(&response_rops, &account_calendar_entry_id));
    assert!(!contains_bytes(&response_rops, &nil_calendar_entry_id));
}

#[tokio::test]
async fn mapi_over_http_hierarchy_synthetic_inbox_default_calendar_entry_id_uses_account_guid() {
    let account = FakeStore::account();
    let store = FakeStore {
        session: Some(account.clone()),
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

    let mut rops = Vec::new();
    append_rop_open_folder(
        &mut rops,
        0,
        1,
        crate::mapi::identity::IPM_SUBTREE_FOLDER_ID,
    );
    rops.extend_from_slice(&[
        0x04, 0x00, 0x01, 0x02, 0x04, // RopGetHierarchyTable
        0x12, 0x00, 0x02, 0x00, // RopSetColumns
    ]);
    rops.extend_from_slice(&2u16.to_le_bytes());
    rops.extend_from_slice(&0x3001_001Fu32.to_le_bytes());
    rops.extend_from_slice(&0x36D0_0102u32.to_le_bytes());
    rops.extend_from_slice(&[
        0x15, 0x00, 0x02, 0x00, 0x01, // RopQueryRows
    ]);
    rops.extend_from_slice(&50u16.to_le_bytes());

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
    let account_calendar_entry_id = crate::mapi::identity::folder_entry_id_from_object_id(
        account.account_id,
        crate::mapi::identity::CALENDAR_FOLDER_ID,
    )
    .unwrap();
    let nil_calendar_entry_id = crate::mapi::identity::folder_entry_id_from_object_id(
        Uuid::nil(),
        crate::mapi::identity::CALENDAR_FOLDER_ID,
    )
    .unwrap();
    assert!(contains_bytes(&response_rops, &utf16z("Inbox")));
    assert!(contains_bytes(&response_rops, &account_calendar_entry_id));
    assert!(!contains_bytes(&response_rops, &nil_calendar_entry_id));
}

#[tokio::test]
async fn mapi_over_http_hierarchy_find_row_default_calendar_entry_id_uses_account_guid() {
    let account = FakeStore::account();
    let store = FakeStore {
        session: Some(account.clone()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            "55555555-5555-5555-5555-555555555555",
            "inbox",
            "Inbox",
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
    let restriction = mapi_content_restriction(0x3001_001F, "Inbox");

    let mut rops = Vec::new();
    append_rop_open_folder(
        &mut rops,
        0,
        1,
        crate::mapi::identity::IPM_SUBTREE_FOLDER_ID,
    );
    rops.extend_from_slice(&[
        0x04, 0x00, 0x01, 0x02, 0x04, // RopGetHierarchyTable
        0x12, 0x00, 0x02, 0x00, // RopSetColumns
    ]);
    rops.extend_from_slice(&2u16.to_le_bytes());
    rops.extend_from_slice(&0x3001_001Fu32.to_le_bytes());
    rops.extend_from_slice(&0x36D0_0102u32.to_le_bytes());
    rops.extend_from_slice(&[
        0x4F, 0x00, 0x02, 0x00, // RopFindRow
    ]);
    rops.extend_from_slice(&(restriction.len() as u16).to_le_bytes());
    rops.extend_from_slice(&restriction);
    rops.push(0);
    rops.extend_from_slice(&0u16.to_le_bytes());

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
    let account_calendar_entry_id = crate::mapi::identity::folder_entry_id_from_object_id(
        account.account_id,
        crate::mapi::identity::CALENDAR_FOLDER_ID,
    )
    .unwrap();
    let nil_calendar_entry_id = crate::mapi::identity::folder_entry_id_from_object_id(
        Uuid::nil(),
        crate::mapi::identity::CALENDAR_FOLDER_ID,
    )
    .unwrap();
    assert!(contains_bytes(
        &response_rops,
        &[0x4F, 0x02, 0, 0, 0, 0, 0, 1]
    ));
    assert!(contains_bytes(&response_rops, &utf16z("Inbox")));
    assert!(contains_bytes(&response_rops, &account_calendar_entry_id));
    assert!(!contains_bytes(&response_rops, &nil_calendar_entry_id));
}

#[tokio::test]
async fn mapi_over_http_hierarchy_find_row_by_inbox_default_calendar_entry_id_matches_real_inbox() {
    let account = FakeStore::account();
    let store = FakeStore {
        session: Some(account.clone()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            "55555555-5555-5555-5555-555555555555",
            "inbox",
            "Inbox",
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
    let calendar_entry_id = crate::mapi::identity::folder_entry_id_from_object_id(
        account.account_id,
        crate::mapi::identity::CALENDAR_FOLDER_ID,
    )
    .unwrap();
    let nil_calendar_entry_id = crate::mapi::identity::folder_entry_id_from_object_id(
        Uuid::nil(),
        crate::mapi::identity::CALENDAR_FOLDER_ID,
    )
    .unwrap();
    let mut restriction = Vec::new();
    append_search_property_binary(&mut restriction, 0x36D0_0102, 0x04, &calendar_entry_id);

    let mut rops = Vec::new();
    append_rop_open_folder(
        &mut rops,
        0,
        1,
        crate::mapi::identity::IPM_SUBTREE_FOLDER_ID,
    );
    rops.extend_from_slice(&[
        0x04, 0x00, 0x01, 0x02, 0x04, // RopGetHierarchyTable
        0x12, 0x00, 0x02, 0x00, // RopSetColumns
    ]);
    rops.extend_from_slice(&2u16.to_le_bytes());
    rops.extend_from_slice(&0x3001_001Fu32.to_le_bytes());
    rops.extend_from_slice(&0x36D0_0102u32.to_le_bytes());
    rops.extend_from_slice(&[
        0x4F, 0x00, 0x02, 0x00, // RopFindRow
    ]);
    rops.extend_from_slice(&(restriction.len() as u16).to_le_bytes());
    rops.extend_from_slice(&restriction);
    rops.push(0);
    rops.extend_from_slice(&0u16.to_le_bytes());

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
    assert!(contains_bytes(
        &response_rops,
        &[0x4F, 0x02, 0, 0, 0, 0, 0, 1]
    ));
    assert!(contains_bytes(&response_rops, &utf16z("Inbox")));
    assert!(contains_bytes(&response_rops, &calendar_entry_id));
    assert!(!contains_bytes(&response_rops, &nil_calendar_entry_id));
}

#[tokio::test]
async fn mapi_over_http_hierarchy_find_row_by_inbox_default_calendar_entry_id_matches_synthetic_inbox(
) {
    let account = FakeStore::account();
    let store = FakeStore {
        session: Some(account.clone()),
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
    let calendar_entry_id = crate::mapi::identity::folder_entry_id_from_object_id(
        account.account_id,
        crate::mapi::identity::CALENDAR_FOLDER_ID,
    )
    .unwrap();
    let nil_calendar_entry_id = crate::mapi::identity::folder_entry_id_from_object_id(
        Uuid::nil(),
        crate::mapi::identity::CALENDAR_FOLDER_ID,
    )
    .unwrap();
    let mut restriction = Vec::new();
    append_search_property_binary(&mut restriction, 0x36D0_0102, 0x04, &calendar_entry_id);

    let mut rops = Vec::new();
    append_rop_open_folder(
        &mut rops,
        0,
        1,
        crate::mapi::identity::IPM_SUBTREE_FOLDER_ID,
    );
    rops.extend_from_slice(&[
        0x04, 0x00, 0x01, 0x02, 0x04, // RopGetHierarchyTable
        0x12, 0x00, 0x02, 0x00, // RopSetColumns
    ]);
    rops.extend_from_slice(&2u16.to_le_bytes());
    rops.extend_from_slice(&0x3001_001Fu32.to_le_bytes());
    rops.extend_from_slice(&0x36D0_0102u32.to_le_bytes());
    rops.extend_from_slice(&[
        0x4F, 0x00, 0x02, 0x00, // RopFindRow
    ]);
    rops.extend_from_slice(&(restriction.len() as u16).to_le_bytes());
    rops.extend_from_slice(&restriction);
    rops.push(0);
    rops.extend_from_slice(&0u16.to_le_bytes());

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
    assert!(contains_bytes(
        &response_rops,
        &[0x4F, 0x02, 0, 0, 0, 0, 0, 1]
    ));
    assert!(contains_bytes(&response_rops, &utf16z("Inbox")));
    assert!(contains_bytes(&response_rops, &calendar_entry_id));
    assert!(!contains_bytes(&response_rops, &nil_calendar_entry_id));
}

#[tokio::test]
async fn mapi_over_http_mailbox_only_account_syncs_empty_contacts_and_calendar() {
    let account = FakeStore::account();
    let store = FakeStore {
        session: Some(account.clone()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            "55555555-5555-5555-5555-555555555555",
            "inbox",
            "Inbox",
        )])),
        ..Default::default()
    };
    let service = ExchangeService::new(store.clone());
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert(
        "cookie",
        HeaderValue::from_str(&mapi_cookie_header(&connect)).unwrap(),
    );

    let mut hierarchy_rops = Vec::new();
    append_rop_open_folder(
        &mut hierarchy_rops,
        0,
        1,
        crate::mapi::identity::IPM_SUBTREE_FOLDER_ID,
    );
    append_rop_outlook_hierarchy_sync_manifest_get_buffer(&mut hierarchy_rops, 1, 2, 4096);
    let hierarchy_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&hierarchy_rops, &[1, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    let hierarchy_rops = response_rops_from_execute_response(hierarchy_response).await;
    let hierarchy = strict_hierarchy_sync_transfer_from_response(&hierarchy_rops).unwrap();
    let contacts = hierarchy
        .folder_changes
        .iter()
        .find(|folder| folder.display_name == "Contacts")
        .expect("Contacts hierarchy row");
    assert_eq!(
        contacts.parent_folder_id,
        Some(crate::mapi::identity::IPM_SUBTREE_FOLDER_ID)
    );
    assert_eq!(contacts.container_class.as_deref(), Some("IPF.Contact"));
    let calendar = hierarchy
        .folder_changes
        .iter()
        .find(|folder| folder.display_name == "Calendar")
        .expect("Calendar hierarchy row");
    assert_eq!(
        calendar.parent_folder_id,
        Some(crate::mapi::identity::IPM_SUBTREE_FOLDER_ID)
    );
    assert_eq!(calendar.container_class.as_deref(), Some("IPF.Appointment"));

    let contacts_rops = content_sync_response_rops_for_store(
        store.clone(),
        crate::mapi::identity::CONTACTS_FOLDER_ID,
        &[],
    )
    .await;
    let contacts_stream = strict_content_sync_transfer_from_response(&contacts_rops).unwrap();
    assert!(contacts_stream.message_changes.is_empty());
    let contacts_checkpoint_id =
        mapi_mailstore::virtual_special_mailbox(crate::mapi::identity::CONTACTS_FOLDER_ID)
            .unwrap()
            .id;
    assert!(store
        .fetch_mapi_sync_checkpoint(
            account.account_id,
            Some(contacts_checkpoint_id),
            MapiCheckpointKind::Content,
        )
        .await
        .unwrap()
        .is_some());

    let calendar_rops = content_sync_response_rops_for_store(
        store.clone(),
        crate::mapi::identity::CALENDAR_FOLDER_ID,
        &[],
    )
    .await;
    let calendar_stream = strict_content_sync_transfer_from_response(&calendar_rops).unwrap();
    assert!(calendar_stream.message_changes.is_empty());
    assert!(!contains_bytes(
        &calendar_rops,
        &utf16z("IPM.Configuration.Calendar")
    ));
    let calendar_checkpoint_id =
        mapi_mailstore::virtual_special_mailbox(crate::mapi::identity::CALENDAR_FOLDER_ID)
            .unwrap()
            .id;
    assert!(store
        .fetch_mapi_sync_checkpoint(
            account.account_id,
            Some(calendar_checkpoint_id),
            MapiCheckpointKind::Content,
        )
        .await
        .unwrap()
        .is_some());
    assert!(store.contact_collections.lock().unwrap().is_empty());
    assert!(store.calendar_collections.lock().unwrap().is_empty());
}

#[tokio::test]
async fn mapi_over_http_custom_only_calendar_collections_keep_default_calendar_openable() {
    let account = FakeStore::account();
    let store = FakeStore {
        session: Some(account.clone()),
        calendar_collections: Arc::new(Mutex::new(vec![FakeStore::collection(
            "team-calendar",
            "calendar",
            "Team Calendar",
        )])),
        ..Default::default()
    };
    let service = ExchangeService::new(store.clone());
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
    append_rop_open_folder(
        &mut rops,
        0,
        1,
        crate::mapi::identity::IPM_SUBTREE_FOLDER_ID,
    );
    rops.extend_from_slice(&[
        0x04, 0x00, 0x01, 0x02, 0x04, // RopGetHierarchyTable
        0x12, 0x00, 0x02, 0x00, // RopSetColumns
    ]);
    rops.extend_from_slice(&7u16.to_le_bytes());
    rops.extend_from_slice(&0x3001_001Fu32.to_le_bytes());
    rops.extend_from_slice(&0x0FFF_0102u32.to_le_bytes());
    rops.extend_from_slice(&0x0FF6_0102u32.to_le_bytes());
    rops.extend_from_slice(&0x65E0_0102u32.to_le_bytes());
    rops.extend_from_slice(&0x3613_001Fu32.to_le_bytes());
    rops.extend_from_slice(&0x36E5_001Eu32.to_le_bytes());
    rops.extend_from_slice(&0x36E5_001Fu32.to_le_bytes());
    rops.extend_from_slice(&[
        0x15, 0x00, 0x02, 0x00, 0x01, // RopQueryRows
    ]);
    rops.extend_from_slice(&50u16.to_le_bytes());
    append_rop_open_folder(&mut rops, 0, 3, crate::mapi::identity::CALENDAR_FOLDER_ID);
    append_rop_get_properties_specific(&mut rops, 3, &[0x3001_001F, 0x3613_001F]);
    rops.extend_from_slice(&[0x05, 0x00, 0x03, 0x04, 0x00]); // RopGetContentsTable

    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(
                &rops,
                &[1, u32::MAX, u32::MAX, u32::MAX, u32::MAX],
            )),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    let response_rops = response_rops_from_execute_response(response).await;
    let rows = hierarchy_query_calendar_contract_rows(&response_rops, 8 + 10 + 7).unwrap();
    let calendar = rows
        .iter()
        .find(|row| row.display_name == "Calendar")
        .expect("default Calendar row missing from hierarchy table");
    assert_eq!(
        calendar.entry_id,
        crate::mapi::identity::folder_entry_id_from_object_id(
            account.account_id,
            crate::mapi::identity::CALENDAR_FOLDER_ID
        )
        .unwrap()
    );
    assert_eq!(
        calendar.instance_key,
        crate::mapi::identity::instance_key_for_object_id(
            crate::mapi::identity::CALENDAR_FOLDER_ID
        )
    );
    assert_eq!(
        calendar.source_key,
        mapi_mailstore::source_key_for_store_id(crate::mapi::identity::CALENDAR_FOLDER_ID)
    );
    assert_eq!(calendar.container_class, "IPF.Appointment");
    assert_eq!(calendar.default_post_message_class_a, "IPM.Appointment");
    assert_eq!(calendar.default_post_message_class_w, "IPM.Appointment");
    let team_calendar = rows
        .iter()
        .find(|row| row.display_name == "Team Calendar")
        .expect("custom calendar row missing from hierarchy table");
    assert_eq!(team_calendar.container_class, "IPF.Appointment");
    assert_eq!(
        team_calendar.default_post_message_class_a,
        "IPM.Appointment"
    );
    assert_eq!(
        team_calendar.default_post_message_class_w,
        "IPM.Appointment"
    );
    assert!(contains_bytes(
        &response_rops,
        &[0x02, 0x03, 0, 0, 0, 0, 0, 0]
    ));
    assert!(contains_bytes(
        &response_rops,
        &[0x05, 0x04, 0, 0, 0, 0, 0, 0, 0, 0]
    ));
    assert_eq!(store.calendar_collections.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn mapi_over_http_get_receive_folder_calendar_fid_opens_default_calendar_with_custom_only_collections(
) {
    let account = FakeStore::account();
    let store = FakeStore {
        session: Some(account.clone()),
        calendar_collections: Arc::new(Mutex::new(vec![FakeStore::collection(
            "team-calendar",
            "calendar",
            "Team Calendar",
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

    let mut rops = vec![0x27, 0x00, 0x00]; // RopGetReceiveFolder.
    rops.extend_from_slice(b"IPM.Appointment\0");
    append_rop_open_folder(&mut rops, 0, 1, crate::mapi::identity::CALENDAR_FOLDER_ID);
    append_rop_get_properties_specific(&mut rops, 1, &[0x3001_001F, 0x3613_001F, 0x36E5_001F]);
    rops.extend_from_slice(&[0x05, 0x00, 0x01, 0x02, 0x00]); // RopGetContentsTable.

    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    let response_rops = response_rops_from_execute_response(response).await;
    assert_eq!(response_rops[0], 0x27);
    assert_eq!(
        crate::mapi::identity::object_id_from_wire_id(&response_rops[6..14]),
        Some(crate::mapi::identity::CALENDAR_FOLDER_ID)
    );
    assert_eq!(&response_rops[14..30], b"IPM.Appointment\0");
    assert_eq!(&response_rops[30..38], &[0x02, 0x01, 0, 0, 0, 0, 0, 0]);
    assert!(contains_bytes(&response_rops, &utf16z("Calendar")));
    assert!(contains_bytes(&response_rops, &utf16z("IPF.Appointment")));
    assert!(contains_bytes(&response_rops, &utf16z("IPM.Appointment")));
    assert!(contains_bytes(
        &response_rops,
        &[0x05, 0x02, 0, 0, 0, 0, 0, 0, 0, 0]
    ));
}

#[test]
fn mapi_over_http_outlook_startup_replay_keeps_calendar_search_and_partial_sync_contracts() {
    std::thread::Builder::new()
        .name("mapi-outlook-startup-replay".to_string())
        .stack_size(16 * 1024 * 1024)
        .spawn(|| {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(async {
    let account = FakeStore::account();
    let inbox_id = Uuid::parse_str("55555555-5555-4555-9555-555555555501").unwrap();
    let trash_id = Uuid::parse_str("77777777-7777-4777-8777-777777777701").unwrap();
    let message_id = Uuid::parse_str("46464646-4646-4646-8646-464646464602").unwrap();
    let inbox = FakeStore::mailbox(&inbox_id.to_string(), "inbox", "Inbox");
    let mut trash = FakeStore::mailbox(&trash_id.to_string(), "trash", "Deleted Items");
    trash.total_emails = 1;
    let store = FakeStore {
        session: Some(account.clone()),
        mailboxes: Arc::new(Mutex::new(vec![inbox.clone(), trash.clone()])),
        emails: Arc::new(Mutex::new(vec![FakeStore::email(
            &message_id.to_string(),
            &trash_id.to_string(),
            "trash",
            "Observed startup trash message",
        )])),
        calendar_collections: Arc::new(Mutex::new(vec![FakeStore::collection(
            "default", "calendar", "Calendar",
        )])),
        search_folders: Arc::new(Mutex::new(vec![
            SearchFolderDefinition {
                id: Uuid::parse_str("34343434-3434-4434-8434-343434343401").unwrap(),
                account_id: account.account_id,
                role: "reminders".to_string(),
                display_name: "Reminders".to_string(),
                definition_kind: "exchange_builtin".to_string(),
                result_object_kind: "mixed".to_string(),
                scope_json: serde_json::json!({"scope": "top_of_personal_folders"}),
                restriction_json: serde_json::json!({"kind": "exchange_reminders"}),
                excluded_folder_roles: exchange_reminder_excluded_folder_roles(),
                is_builtin: true,
            },
            SearchFolderDefinition {
                id: Uuid::parse_str("34343434-3434-4434-8434-343434343402").unwrap(),
                account_id: account.account_id,
                role: "contacts_search".to_string(),
                display_name: "Contacts Search".to_string(),
                definition_kind: "exchange_builtin".to_string(),
                result_object_kind: "contact".to_string(),
                scope_json: serde_json::json!({"scope": "contacts"}),
                restriction_json: serde_json::json!({"kind": "contacts_search"}),
                excluded_folder_roles: Vec::new(),
                is_builtin: true,
            },
            SearchFolderDefinition {
                id: Uuid::parse_str("34343434-3434-4434-8434-343434343403").unwrap(),
                account_id: account.account_id,
                role: "tracked_mail_processing".to_string(),
                display_name: "Tracked Mail Processing".to_string(),
                definition_kind: "exchange_builtin".to_string(),
                result_object_kind: "message".to_string(),
                scope_json: serde_json::json!({"scope": "mail"}),
                restriction_json: serde_json::json!({"kind": "tracked_mail_processing"}),
                excluded_folder_roles: Vec::new(),
                is_builtin: true,
            },
            SearchFolderDefinition {
                id: Uuid::parse_str("34343434-3434-4434-8434-343434343404").unwrap(),
                account_id: account.account_id,
                role: "todo_search".to_string(),
                display_name: "To-Do".to_string(),
                definition_kind: "exchange_builtin".to_string(),
                result_object_kind: "task".to_string(),
                scope_json: serde_json::json!({"scope": "tasks"}),
                restriction_json: serde_json::json!({"kind": "todo_search"}),
                excluded_folder_roles: Vec::new(),
                is_builtin: true,
            },
        ])),
        ..Default::default()
    };
    store
        .store_mapi_sync_checkpoint(
            account.account_id,
            Some(trash_id),
            MapiCheckpointKind::Content,
            88,
            44,
            serde_json::json!({"source": "full-trash-content-sync"}),
        )
        .await
        .unwrap();
    let calendar_checkpoint_id =
        mapi_mailstore::virtual_special_mailbox(crate::mapi::identity::CALENDAR_FOLDER_ID)
            .unwrap()
            .id;
    store
        .store_mapi_sync_checkpoint(
            account.account_id,
            Some(calendar_checkpoint_id),
            MapiCheckpointKind::Content,
            89,
            45,
            serde_json::json!({
                "source": "emsmdb-ics-download",
                "syncRootFolderId": crate::mapi::identity::CALENDAR_FOLDER_ID
            }),
        )
        .await
        .unwrap();
    *store.mapi_sync_changes.lock().unwrap() = MapiSyncChangeSet {
        current_change_sequence: 89,
        current_modseq: 45,
        changed_message_ids: vec![message_id],
        ..Default::default()
    };
    let service = ExchangeService::new(store.clone());

    let nspi_headers = nspi_bound_headers(&service, "DNToMId").await;
    let nspi_dn_to_mid = service
        .handle_mapi(MapiEndpoint::Nspi, &nspi_headers, b"alice@example.test\0")
        .await
        .unwrap();
    assert_eq!(nspi_dn_to_mid.headers().get("x-responsecode").unwrap(), "0");
    let nspi_dn_to_mid_body = response_bytes(nspi_dn_to_mid).await;
    let principal_mid = u32::from_le_bytes(nspi_dn_to_mid_body[13..17].try_into().unwrap());
    assert_ne!(principal_mid, 0);

    let mut nspi_get_props_request = Vec::new();
    nspi_get_props_request.extend_from_slice(&principal_mid.to_le_bytes());
    nspi_get_props_request.extend_from_slice(&0x8C6D_0102u32.to_le_bytes());
    let nspi_headers = nspi_bound_headers(&service, "GetProps").await;
    let nspi_guid = service
        .handle_mapi(MapiEndpoint::Nspi, &nspi_headers, &nspi_get_props_request)
        .await
        .unwrap();
    let nspi_guid_body = response_bytes(nspi_guid).await;
    assert!(contains_bytes(
        &nspi_guid_body,
        account.account_id.to_bytes_le().as_slice()
    ));

    let mut nspi_smtp_request = Vec::new();
    nspi_smtp_request.extend_from_slice(&principal_mid.to_le_bytes());
    nspi_smtp_request.extend_from_slice(&0x39FE_001Fu32.to_le_bytes());
    let nspi_smtp = service
        .handle_mapi(MapiEndpoint::Nspi, &nspi_headers, &nspi_smtp_request)
        .await
        .unwrap();
    let nspi_smtp_body = response_bytes(nspi_smtp).await;
    assert!(contains_bytes(
        &nspi_smtp_body,
        &utf16z("alice@example.test")
    ));

    let mut nspi_email_request = Vec::new();
    nspi_email_request.extend_from_slice(&principal_mid.to_le_bytes());
    nspi_email_request.extend_from_slice(&0x3003_001Fu32.to_le_bytes());
    let nspi_email = service
        .handle_mapi(MapiEndpoint::Nspi, &nspi_headers, &nspi_email_request)
        .await
        .unwrap();
    let nspi_email_body = response_bytes(nspi_email).await;
    assert!(contains_bytes(
        &nspi_email_body,
        &utf16z(&test_account_legacy_dn("alice@example.test"))
    ));
    assert!(!contains_bytes(
        &nspi_email_body,
        &utf16z("alice@example.test")
    ));

    let mut nspi_query_rows_request = hex_bytes(
        "00000000ff000000000000000000000000000000000000000000000000e40400000904000009080000\
         010000002600008001000000ff0b0000000201ff0f1f0001300300fe0f030000391f00203a\
         1f0003301f0002300b00403a1f00ff391f00",
    );
    nspi_query_rows_request[49..53].copy_from_slice(&principal_mid.to_le_bytes());
    let nspi_headers = nspi_bound_headers(&service, "QueryRows").await;
    let nspi_query_rows = service
        .handle_mapi(MapiEndpoint::Nspi, &nspi_headers, &nspi_query_rows_request)
        .await
        .unwrap();
    let nspi_query_rows_body = response_bytes(nspi_query_rows).await;
    assert!(contains_bytes(
        &nspi_query_rows_body,
        &utf16z("alice@example.test")
    ));
    assert!(contains_bytes(
        &nspi_query_rows_body,
        &utf16z(&test_account_legacy_dn("alice@example.test"))
    ));
    assert!(contains_bytes(&nspi_query_rows_body, &utf16z("alice")));
    assert!(contains_bytes(&nspi_query_rows_body, &utf16z("EX")));
    assert!(!contains_bytes(&nspi_query_rows_body, &utf16z("SMTP")));

    let bootstrap_connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    assert_eq!(
        bootstrap_connect.headers().get("x-responsecode").unwrap(),
        "0"
    );
    let mut bootstrap_headers = mapi_headers("Execute");
    bootstrap_headers.insert(
        "cookie",
        HeaderValue::from_str(&mapi_cookie_header(&bootstrap_connect)).unwrap(),
    );
    let mut bootstrap_rops = vec![
        0xFE, 0x00, 0x00, 0x01, // RopLogon
    ];
    bootstrap_rops.extend_from_slice(&0u32.to_le_bytes());
    bootstrap_rops.extend_from_slice(&0u32.to_le_bytes());
    bootstrap_rops.extend_from_slice(&0u16.to_le_bytes());
    let bootstrap_logon = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &bootstrap_headers,
            &execute_body(&rop_buffer(&bootstrap_rops, &[u32::MAX])),
        )
        .await
        .unwrap();
    assert_eq!(bootstrap_logon.status(), StatusCode::OK);
    assert_eq!(
        bootstrap_logon.headers().get("x-responsecode").unwrap(),
        "0"
    );
    let mut bootstrap_cookie = mapi_cookie_header(&bootstrap_logon);
    let bootstrap_logon_rops = response_rops_from_execute_response(bootstrap_logon).await;
    assert!(contains_bytes(
        &bootstrap_logon_rops,
        &[0xFE, 0x00, 0, 0, 0, 0]
    ));

    let mut bootstrap_headers = mapi_headers("Execute");
    bootstrap_headers.insert("cookie", HeaderValue::from_str(&bootstrap_cookie).unwrap());
    let mut bootstrap_getprops = Vec::new();
    append_rop_get_properties_specific(
        &mut bootstrap_getprops,
        0,
        &[
            0x6638_0102, // PidTagSerializedReplidGuidMap
        ],
    );
    let bootstrap_getprops_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &bootstrap_headers,
            &execute_body(&rop_buffer(&bootstrap_getprops, &[1])),
        )
        .await
        .unwrap();
    assert_eq!(bootstrap_getprops_response.status(), StatusCode::OK);
    assert_eq!(
        bootstrap_getprops_response
            .headers()
            .get("x-responsecode")
            .unwrap(),
        "0"
    );
    bootstrap_cookie = mapi_cookie_header(&bootstrap_getprops_response);
    let bootstrap_getprops_rops =
        response_rops_from_execute_response(bootstrap_getprops_response).await;
    assert!(contains_bytes(
        &bootstrap_getprops_rops,
        &[0x07, 0x00, 0, 0, 0, 0]
    ));
    assert!(contains_bytes(
        &bootstrap_getprops_rops,
        &crate::mapi::identity::STORE_REPLICA_GUID[..4]
    ));

    let mut bootstrap_headers = mapi_headers("Execute");
    bootstrap_headers.insert("cookie", HeaderValue::from_str(&bootstrap_cookie).unwrap());
    let mut bootstrap_named_props = vec![
        0x56, 0x00, 0x00, 0x02, // RopGetPropertyIdsFromNames, create missing on Logon
    ];
    bootstrap_named_props.extend_from_slice(&2u16.to_le_bytes());
    for lid in [0x8580u32, 0x8581u32] {
        bootstrap_named_props.push(0x00);
        bootstrap_named_props.extend_from_slice(&[
            0x08, 0x20, 0x06, 0x00, 0x00, 0x00, 0x00, 0x00, 0xC0, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x46,
        ]);
        bootstrap_named_props.extend_from_slice(&lid.to_le_bytes());
    }
    let bootstrap_named_props_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &bootstrap_headers,
            &execute_body(&rop_buffer(&bootstrap_named_props, &[1])),
        )
        .await
        .unwrap();
    assert_eq!(bootstrap_named_props_response.status(), StatusCode::OK);
    assert_eq!(
        bootstrap_named_props_response
            .headers()
            .get("x-responsecode")
            .unwrap(),
        "0"
    );
    bootstrap_cookie = mapi_cookie_header(&bootstrap_named_props_response);
    let bootstrap_named_props_rops =
        response_rops_from_execute_response(bootstrap_named_props_response).await;
    assert!(contains_bytes(
        &bootstrap_named_props_rops,
        &[0x56, 0x00, 0, 0, 0, 0]
    ));
    assert_eq!(
        u16::from_le_bytes(bootstrap_named_props_rops[6..8].try_into().unwrap()),
        2
    );
    let first_property_id =
        u16::from_le_bytes(bootstrap_named_props_rops[8..10].try_into().unwrap());
    let second_property_id =
        u16::from_le_bytes(bootstrap_named_props_rops[10..12].try_into().unwrap());
    assert!(first_property_id > 0x8000 && first_property_id != 0xffff);
    assert!(second_property_id > 0x8000 && second_property_id != 0xffff);
    assert_ne!(first_property_id, second_property_id);
    assert_eq!(first_property_id, 0x9001);
    assert_eq!(second_property_id, 0x9002);

    let mut bootstrap_headers = mapi_headers("Execute");
    bootstrap_headers.insert("cookie", HeaderValue::from_str(&bootstrap_cookie).unwrap());
    let bootstrap_address_types = vec![
        0x49, 0x00, 0x00, // RopGetAddressTypes
    ];
    let bootstrap_address_types_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &bootstrap_headers,
            &execute_body(&rop_buffer(&bootstrap_address_types, &[1])),
        )
        .await
        .unwrap();
    assert_eq!(bootstrap_address_types_response.status(), StatusCode::OK);
    assert_eq!(
        bootstrap_address_types_response
            .headers()
            .get("x-responsecode")
            .unwrap(),
        "0"
    );
    bootstrap_cookie = mapi_cookie_header(&bootstrap_address_types_response);
    let bootstrap_address_types_rops =
        response_rops_from_execute_response(bootstrap_address_types_response).await;
    assert!(contains_bytes(
        &bootstrap_address_types_rops,
        &[0x49, 0x00, 0, 0, 0, 0]
    ));
    assert!(contains_bytes(&bootstrap_address_types_rops, b"EX\0SMTP\0"));

    let mut bootstrap_headers = mapi_headers("Execute");
    bootstrap_headers.insert("cookie", HeaderValue::from_str(&bootstrap_cookie).unwrap());
    let mut bootstrap_store_props = Vec::new();
    append_rop_get_properties_specific(
        &mut bootstrap_store_props,
        0,
        &[
            0x661C_001F, // PidTagMailboxOwnerName
            0x661B_0102, // PidTagMailboxOwnerEntryId
            0x3009_0003, // PidTagResourceFlags
            0x6619_0102, // PidTagUserEntryId
            0x6631_0102, // PidTagIpmPublicFoldersEntryId
            0x341D_001F, // PidTagServerTypeDisplayName
            0x341E_0102, // PidTagServerConnectedIcon
            0x341F_0102, // PidTagServerAccountIcon
            0x0E5C_000B, // PidTagPrivate
            0x346F_0003, // PidTagOutlookStoreState
            0x6707_0102, // PidTagUserGuid
        ],
    );
    let bootstrap_store_props_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &bootstrap_headers,
            &execute_body(&rop_buffer(&bootstrap_store_props, &[1])),
        )
        .await
        .unwrap();
    assert_eq!(bootstrap_store_props_response.status(), StatusCode::OK);
    assert_eq!(
        bootstrap_store_props_response
            .headers()
            .get("x-responsecode")
            .unwrap(),
        "0"
    );
    bootstrap_cookie = mapi_cookie_header(&bootstrap_store_props_response);
    let bootstrap_store_props_rops =
        response_rops_from_execute_response(bootstrap_store_props_response).await;
    assert!(contains_bytes(
        &bootstrap_store_props_rops,
        &[0x07, 0x00, 0, 0, 0, 0]
    ));
    assert!(contains_bytes(
        &bootstrap_store_props_rops,
        &utf16z("Alice")
    ));
    assert!(contains_bytes(&bootstrap_store_props_rops, &utf16z("LPE")));
    assert!(contains_bytes(
        &bootstrap_store_props_rops,
        &[0x00, 0x00, 0x01, 0x00, 0x01, 0x00, 0x01, 0x01]
    ));
    assert!(contains_bytes(
        &bootstrap_store_props_rops,
        &[0x30, 0x00, 0x00, 0x00, 0x16, 0x00, 0x00, 0x00]
    ));

    let mut trace_store_props_headers = mapi_headers("Execute");
    trace_store_props_headers.insert("cookie", HeaderValue::from_str(&bootstrap_cookie).unwrap());
    let mut trace_store_props = Vec::new();
    append_rop_get_properties_specific(
        &mut trace_store_props,
        0,
        &[
            0x0E5C_000B, // PidTagPrivate
            0x3009_0003, // PidTagResourceFlags
            0x6619_0102, // PidTagUserEntryId
            0x661B_0102, // PidTagMailboxOwnerEntryId
            0x661C_001F, // PidTagMailboxOwnerName
            0x0EA0_0048, // PidTagAssociatedSharingProvider
            0x6631_0102, // PidTagIpmPublicFoldersEntryId
        ],
    );
    let trace_store_props_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &trace_store_props_headers,
            &execute_body(&rop_buffer(&trace_store_props, &[1])),
        )
        .await
        .unwrap();
    assert_eq!(trace_store_props_response.status(), StatusCode::OK);
    assert_eq!(
        trace_store_props_response
            .headers()
            .get("x-responsecode")
            .unwrap(),
        "0"
    );
    bootstrap_cookie = mapi_cookie_header(&trace_store_props_response);
    let trace_store_props_rops =
        response_rops_from_execute_response(trace_store_props_response).await;
    assert!(contains_bytes(
        &trace_store_props_rops,
        &[
            0xAE, 0xF0, 0x06, 0x00, 0x00, 0x00, 0x00, 0x00, 0xC0, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x46,
        ]
    ));
    let public_folders_entry_id = crate::mapi::identity::folder_entry_id_from_object_id(
        account.account_id,
        crate::mapi::identity::PUBLIC_FOLDERS_ROOT_FOLDER_ID,
    )
    .unwrap();
    assert!(contains_bytes(
        &trace_store_props_rops,
        &public_folders_entry_id
    ));
    assert!(!contains_bytes(
        &trace_store_props_rops,
        &[0x02, 0x01, 0x04, 0x80]
    ));

    let mut cookie = bootstrap_cookie;

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let mut hierarchy_rops = Vec::new();
    append_rop_open_folder(
        &mut hierarchy_rops,
        0,
        1,
        crate::mapi::identity::IPM_SUBTREE_FOLDER_ID,
    );
    append_rop_outlook_hierarchy_sync_manifest_get_buffer(&mut hierarchy_rops, 1, 2, 4096);
    let hierarchy_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&hierarchy_rops, &[1, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    cookie = mapi_cookie_header(&hierarchy_response);
    let hierarchy_rops = response_rops_from_execute_response(hierarchy_response).await;
    let hierarchy = strict_hierarchy_sync_transfer_from_response(&hierarchy_rops).unwrap();
    let calendar = hierarchy
        .folder_changes
        .iter()
        .find(|folder| folder.display_name == "Calendar")
        .expect("Calendar hierarchy row");
    let calendar_source_key = calendar.source_key.clone();
    assert_eq!(
        calendar.parent_folder_id,
        Some(crate::mapi::identity::IPM_SUBTREE_FOLDER_ID)
    );
    assert_eq!(calendar.container_class.as_deref(), Some("IPF.Appointment"));
    assert!(!hierarchy
        .folder_changes
        .iter()
        .any(|folder| folder.display_name == "Reminders"));

    let hierarchy_checkpoint = store
        .fetch_mapi_sync_checkpoint(account.account_id, None, MapiCheckpointKind::Hierarchy)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(hierarchy_checkpoint.last_change_sequence, 89);
    assert_eq!(hierarchy_checkpoint.last_modseq, 45);

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let mut max_submit_rops = Vec::new();
    append_rop_get_properties_specific(
        &mut max_submit_rops,
        0,
        &[0x666D_0003], // PidTagMaxSubmitMessageSize
    );
    let max_submit_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&max_submit_rops, &[1])),
        )
        .await
        .unwrap();
    cookie = mapi_cookie_header(&max_submit_response);
    let max_submit_rops = response_rops_from_execute_response(max_submit_response).await;
    assert!(contains_bytes(&max_submit_rops, &[0x07, 0x00, 0, 0, 0, 0]));
    assert!(contains_bytes(&max_submit_rops, &35840u32.to_le_bytes()));

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let mut default_probe_rops = vec![0xFE, 0x00, 0x00, 0x01];
    default_probe_rops.extend_from_slice(&0u32.to_le_bytes());
    default_probe_rops.extend_from_slice(&0u32.to_le_bytes());
    default_probe_rops.extend_from_slice(&0u16.to_le_bytes());
    append_rop_get_properties_specific(&mut default_probe_rops, 0, &[0x36D0_0102]);
    append_rop_open_folder(
        &mut default_probe_rops,
        0,
        1,
        crate::mapi::identity::CALENDAR_FOLDER_ID,
    );
    append_rop_get_properties_specific(&mut default_probe_rops, 1, &[0x3001_001F, 0x3613_001F]);
    let default_probe = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&default_probe_rops, &[u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    cookie = mapi_cookie_header(&default_probe);
    let default_probe_rops = response_rops_from_execute_response(default_probe).await;
    assert!(contains_bytes(
        &default_probe_rops,
        &[0x02, 0x01, 0, 0, 0, 0, 0, 0]
    ));
    assert!(contains_bytes(&default_probe_rops, &utf16z("Calendar")));
    assert!(contains_bytes(
        &default_probe_rops,
        &utf16z("IPF.Appointment")
    ));

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let mut calendar_rops = Vec::new();
    append_rop_open_folder(
        &mut calendar_rops,
        0,
        1,
        crate::mapi::identity::CALENDAR_FOLDER_ID,
    );
    calendar_rops.extend_from_slice(&[
        0x70, 0x00, 0x01, 0x02, // RopSynchronizationConfigure
        0x01, 0x00, 0x10, 0x00, // content sync, FAI only
        0x00, 0x00, // RestrictionDataSize
        0x0d, 0x00, 0x00, 0x00, // SynchronizationExtraFlags: Eid | MessageSize | CN
        0x00, 0x00, // PropertyTagCount
        0x75, 0x00, 0x02, // RopSynchronizationUploadStateStreamBegin
    ]);
    calendar_rops.extend_from_slice(&0x4017_0102u32.to_le_bytes());
    calendar_rops.extend_from_slice(&0u32.to_le_bytes());
    calendar_rops.extend_from_slice(&[
        0x77, 0x00, 0x02, // RopSynchronizationUploadStateStreamEnd
        0x75, 0x00, 0x02, // RopSynchronizationUploadStateStreamBegin
    ]);
    calendar_rops.extend_from_slice(&0x6796_0102u32.to_le_bytes());
    calendar_rops.extend_from_slice(&0u32.to_le_bytes());
    calendar_rops.extend_from_slice(&[
        0x77, 0x00, 0x02, // RopSynchronizationUploadStateStreamEnd
        0x75, 0x00, 0x02, // RopSynchronizationUploadStateStreamBegin
    ]);
    calendar_rops.extend_from_slice(&0x67DA_0102u32.to_le_bytes());
    calendar_rops.extend_from_slice(&0u32.to_le_bytes());
    calendar_rops.extend_from_slice(&[
        0x77, 0x00, 0x02, // RopSynchronizationUploadStateStreamEnd
        0x75, 0x00, 0x02, // RopSynchronizationUploadStateStreamBegin
    ]);
    calendar_rops.extend_from_slice(&0x67D2_0102u32.to_le_bytes());
    calendar_rops.extend_from_slice(&0u32.to_le_bytes());
    calendar_rops.extend_from_slice(&[
        0x77, 0x00, 0x02, // RopSynchronizationUploadStateStreamEnd
        0x4E, 0x00, 0x02, // RopFastTransferSourceGetBuffer
    ]);
    calendar_rops.extend_from_slice(&4096u16.to_le_bytes());
    let calendar_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&calendar_rops, &[1, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    cookie = mapi_cookie_header(&calendar_response);
    let calendar_rops = response_rops_from_execute_response(calendar_response).await;
    let calendar_stream = strict_content_sync_transfer_from_response(&calendar_rops).unwrap();
    assert!(calendar_stream.message_changes.is_empty());
    assert!(!contains_bytes(
        &calendar_rops,
        &utf16z("IPM.Configuration.Calendar")
    ));
    assert!(store
        .fetch_mapi_sync_checkpoint(
            account.account_id,
            Some(calendar_checkpoint_id),
            MapiCheckpointKind::Content,
        )
        .await
        .unwrap()
        .is_some());

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let mut trash_rops = Vec::new();
    append_rop_open_folder(
        &mut trash_rops,
        0,
        1,
        crate::mapi::identity::TRASH_FOLDER_ID,
    );
    trash_rops.extend_from_slice(&[
        0x70, 0x00, 0x01, 0x02, 0x01, 0x00, 0x10, 0x00, 0x00, 0x00, 0x05, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x82, 0x00, 0x02, 0x03, 0x4E, 0x00, 0x03,
    ]);
    trash_rops.extend_from_slice(&4096u16.to_le_bytes());
    let trash_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&trash_rops, &[1, u32::MAX, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    cookie = mapi_cookie_header(&trash_response);
    let trash_rops = response_rops_from_execute_response(trash_response).await;
    assert!(!contains_bytes(
        &trash_rops,
        &utf16z("Observed startup trash message")
    ));
    let trash_checkpoint = store
        .fetch_mapi_sync_checkpoint(
            account.account_id,
            Some(trash_id),
            MapiCheckpointKind::Content,
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(trash_checkpoint.last_change_sequence, 88);
    assert_eq!(trash_checkpoint.last_modseq, 44);

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let mut root_rops = Vec::new();
    append_rop_open_folder(&mut root_rops, 0, 1, crate::mapi::identity::ROOT_FOLDER_ID);
    append_rop_outlook_hierarchy_sync_manifest_get_buffer(&mut root_rops, 1, 2, 20000);
    let root_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&root_rops, &[1, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    cookie = mapi_cookie_header(&root_response);
    let root_rops = response_rops_from_execute_response(root_response).await;
    let root_hierarchy = strict_hierarchy_sync_transfer_from_response(&root_rops).unwrap();
    for name in ["Reminders", "Tracked Mail Processing", "To-Do"] {
        let folder = root_hierarchy
            .folder_changes
            .iter()
            .find(|folder| folder.display_name == name)
            .unwrap_or_else(|| panic!("{name} hierarchy row"));
        assert_eq!(
            folder.parent_folder_id,
            Some(crate::mapi::identity::ROOT_FOLDER_ID)
        );
    }

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let mut common_views_rops = Vec::new();
    append_rop_open_folder(
        &mut common_views_rops,
        0,
        1,
        crate::mapi::identity::COMMON_VIEWS_FOLDER_ID,
    );
    common_views_rops.extend_from_slice(&[
        0x70, 0x00, 0x01, 0x02, // RopSynchronizationConfigure
        0x01, 0x00, 0x39, 0xA1, // content sync, observed Outlook flags 0xa139
        0x00, 0x00, // RestrictionDataSize
        0x0d, 0x00, 0x00, 0x00, // SynchronizationExtraFlags: Eid | MessageSize | CN
        0x00, 0x00, // PropertyTagCount
    ]);
    for state_tag in [0x4017_0102u32, 0x6796_0102, 0x67DA_0102, 0x67D2_0102] {
        common_views_rops.extend_from_slice(&[0x75, 0x00, 0x02]);
        common_views_rops.extend_from_slice(&state_tag.to_le_bytes());
        common_views_rops.extend_from_slice(&0u32.to_le_bytes());
        common_views_rops.extend_from_slice(&[0x77, 0x00, 0x02]);
    }
    common_views_rops.extend_from_slice(&[0x4E, 0x00, 0x02]);
    common_views_rops.extend_from_slice(&31680u16.to_le_bytes());
    let common_views_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&common_views_rops, &[1, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    cookie = mapi_cookie_header(&common_views_response);
    let common_views_rops = response_rops_from_execute_response(common_views_response).await;
    let common_views_stream =
        strict_content_sync_transfer_from_response(&common_views_rops).unwrap();
    assert_eq!(common_views_stream.message_changes.len(), 2);
    assert!(common_views_stream
        .message_changes
        .iter()
        .all(|message| message.associated));
    assert!(common_views_stream
        .message_changes
        .iter()
        .any(|message| message.subject == "Compact"));
    assert!(common_views_stream
        .message_changes
        .iter()
        .any(|message| message.subject == "Sent To"));
    assert!(contains_bytes(
        &common_views_rops,
        &utf16z("IPM.Microsoft.FolderDesign.NamedView")
    ));
    assert!(!contains_bytes(
        &common_views_rops,
        &utf16z("IPM.Microsoft.WunderBar.SFInfo")
    ));

    let mut disconnect_headers = mapi_headers("Disconnect");
    disconnect_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let disconnect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &disconnect_headers, b"")
        .await
        .unwrap();
    assert_eq!(disconnect.status(), StatusCode::OK);
    assert_eq!(disconnect.headers().get("x-responsecode").unwrap(), "0");

    let reconnect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    assert_eq!(reconnect.status(), StatusCode::OK);
    assert_eq!(reconnect.headers().get("x-responsecode").unwrap(), "0");
    cookie = mapi_cookie_header(&reconnect);

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let mut reconnect_hierarchy_rops = Vec::new();
    append_rop_open_folder(
        &mut reconnect_hierarchy_rops,
        0,
        1,
        crate::mapi::identity::IPM_SUBTREE_FOLDER_ID,
    );
    append_rop_outlook_hierarchy_sync_manifest_get_buffer(
        &mut reconnect_hierarchy_rops,
        1,
        2,
        4096,
    );
    let reconnect_hierarchy_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(
                &reconnect_hierarchy_rops,
                &[1, u32::MAX, u32::MAX],
            )),
        )
        .await
        .unwrap();
    cookie = mapi_cookie_header(&reconnect_hierarchy_response);
    let reconnect_hierarchy_rops =
        response_rops_from_execute_response(reconnect_hierarchy_response).await;
    let reconnect_hierarchy =
        strict_hierarchy_sync_transfer_from_response(&reconnect_hierarchy_rops).unwrap();
    let reconnect_calendar = reconnect_hierarchy
        .folder_changes
        .iter()
        .find(|folder| folder.display_name == "Calendar")
        .expect("reconnected Calendar hierarchy row");
    assert_eq!(reconnect_calendar.source_key, calendar_source_key);
    assert_eq!(
        reconnect_calendar.parent_folder_id,
        Some(crate::mapi::identity::IPM_SUBTREE_FOLDER_ID)
    );
    assert_eq!(
        reconnect_calendar.container_class.as_deref(),
        Some("IPF.Appointment")
    );

    let reconnect_hierarchy_checkpoint = store
        .fetch_mapi_sync_checkpoint(account.account_id, None, MapiCheckpointKind::Hierarchy)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(reconnect_hierarchy_checkpoint.last_change_sequence, 89);
    assert_eq!(reconnect_hierarchy_checkpoint.last_modseq, 45);

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let mut reconnect_default_probe_rops = vec![0xFE, 0x00, 0x00, 0x01];
    reconnect_default_probe_rops.extend_from_slice(&0u32.to_le_bytes());
    reconnect_default_probe_rops.extend_from_slice(&0u32.to_le_bytes());
    reconnect_default_probe_rops.extend_from_slice(&0u16.to_le_bytes());
    append_rop_get_properties_specific(&mut reconnect_default_probe_rops, 0, &[0x36D0_0102]);
    append_rop_open_folder(
        &mut reconnect_default_probe_rops,
        0,
        1,
        crate::mapi::identity::CALENDAR_FOLDER_ID,
    );
    append_rop_get_properties_specific(
        &mut reconnect_default_probe_rops,
        1,
        &[0x3001_001F, 0x3613_001F],
    );
    let reconnect_default_probe = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(
                &reconnect_default_probe_rops,
                &[u32::MAX, u32::MAX],
            )),
        )
        .await
        .unwrap();
    cookie = mapi_cookie_header(&reconnect_default_probe);
    let reconnect_default_probe_rops =
        response_rops_from_execute_response(reconnect_default_probe).await;
    assert!(contains_bytes(
        &reconnect_default_probe_rops,
        &[0x02, 0x01, 0, 0, 0, 0, 0, 0]
    ));
    assert!(contains_bytes(
        &reconnect_default_probe_rops,
        &utf16z("Calendar")
    ));
    assert!(contains_bytes(
        &reconnect_default_probe_rops,
        &utf16z("IPF.Appointment")
    ));

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let mut reconnect_calendar_rops = Vec::new();
    append_rop_open_folder(
        &mut reconnect_calendar_rops,
        0,
        1,
        crate::mapi::identity::CALENDAR_FOLDER_ID,
    );
    reconnect_calendar_rops.extend_from_slice(&[
        0x70, 0x00, 0x01, 0x02, 0x01, 0x00, 0x10, 0x00, 0x00, 0x00, 0x0d, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x75, 0x00, 0x02,
    ]);
    reconnect_calendar_rops.extend_from_slice(&0x4017_0102u32.to_le_bytes());
    reconnect_calendar_rops.extend_from_slice(&0u32.to_le_bytes());
    reconnect_calendar_rops.extend_from_slice(&[0x77, 0x00, 0x02, 0x75, 0x00, 0x02]);
    reconnect_calendar_rops.extend_from_slice(&0x6796_0102u32.to_le_bytes());
    reconnect_calendar_rops.extend_from_slice(&0u32.to_le_bytes());
    reconnect_calendar_rops.extend_from_slice(&[0x77, 0x00, 0x02, 0x75, 0x00, 0x02]);
    reconnect_calendar_rops.extend_from_slice(&0x67DA_0102u32.to_le_bytes());
    reconnect_calendar_rops.extend_from_slice(&0u32.to_le_bytes());
    reconnect_calendar_rops.extend_from_slice(&[0x77, 0x00, 0x02, 0x75, 0x00, 0x02]);
    reconnect_calendar_rops.extend_from_slice(&0x67D2_0102u32.to_le_bytes());
    reconnect_calendar_rops.extend_from_slice(&0u32.to_le_bytes());
    reconnect_calendar_rops.extend_from_slice(&[0x77, 0x00, 0x02, 0x4E, 0x00, 0x02]);
    reconnect_calendar_rops.extend_from_slice(&4096u16.to_le_bytes());
    let reconnect_calendar_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(
                &reconnect_calendar_rops,
                &[1, u32::MAX, u32::MAX],
            )),
        )
        .await
        .unwrap();
    cookie = mapi_cookie_header(&reconnect_calendar_response);
    let reconnect_calendar_rops =
        response_rops_from_execute_response(reconnect_calendar_response).await;
    let reconnect_calendar_stream =
        strict_content_sync_transfer_from_response(&reconnect_calendar_rops).unwrap();
    assert!(reconnect_calendar_stream.message_changes.is_empty());
    assert!(!contains_bytes(
        &reconnect_calendar_rops,
        &utf16z("IPM.Configuration.Calendar")
    ));
    let reconnected_calendar_checkpoint = store
        .fetch_mapi_sync_checkpoint(
            account.account_id,
            Some(calendar_checkpoint_id),
            MapiCheckpointKind::Content,
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(reconnected_calendar_checkpoint.last_change_sequence, 89);
    assert_eq!(reconnected_calendar_checkpoint.last_modseq, 45);

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let mut reconnect_trash_rops = Vec::new();
    append_rop_open_folder(
        &mut reconnect_trash_rops,
        0,
        1,
        crate::mapi::identity::TRASH_FOLDER_ID,
    );
    reconnect_trash_rops.extend_from_slice(&[
        0x70, 0x00, 0x01, 0x02, 0x01, 0x00, 0x10, 0x00, 0x00, 0x00, 0x05, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x82, 0x00, 0x02, 0x03, 0x4E, 0x00, 0x03,
    ]);
    reconnect_trash_rops.extend_from_slice(&4096u16.to_le_bytes());
    let reconnect_trash_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(
                &reconnect_trash_rops,
                &[1, u32::MAX, u32::MAX, u32::MAX],
            )),
        )
        .await
        .unwrap();
    cookie = mapi_cookie_header(&reconnect_trash_response);
    let reconnect_trash_rops = response_rops_from_execute_response(reconnect_trash_response).await;
    assert!(!contains_bytes(
        &reconnect_trash_rops,
        &utf16z("Observed startup trash message")
    ));
    let reconnect_trash_checkpoint = store
        .fetch_mapi_sync_checkpoint(
            account.account_id,
            Some(trash_id),
            MapiCheckpointKind::Content,
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(reconnect_trash_checkpoint.last_change_sequence, 88);
    assert_eq!(reconnect_trash_checkpoint.last_modseq, 44);

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let mut reconnect_root_rops = Vec::new();
    append_rop_open_folder(
        &mut reconnect_root_rops,
        0,
        1,
        crate::mapi::identity::ROOT_FOLDER_ID,
    );
    append_rop_outlook_hierarchy_sync_manifest_get_buffer(&mut reconnect_root_rops, 1, 2, 20000);
    let reconnect_root_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&reconnect_root_rops, &[1, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    cookie = mapi_cookie_header(&reconnect_root_response);
    let reconnect_root_rops = response_rops_from_execute_response(reconnect_root_response).await;
    let reconnect_root_hierarchy =
        strict_hierarchy_sync_transfer_from_response(&reconnect_root_rops).unwrap();
    for name in ["Reminders", "Tracked Mail Processing", "To-Do"] {
        let folder = reconnect_root_hierarchy
            .folder_changes
            .iter()
            .find(|folder| folder.display_name == name)
            .unwrap_or_else(|| panic!("reconnected {name} hierarchy row"));
        assert_eq!(
            folder.parent_folder_id,
            Some(crate::mapi::identity::ROOT_FOLDER_ID)
        );
    }

    let mut reconnect_disconnect_headers = mapi_headers("Disconnect");
    reconnect_disconnect_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let reconnect_disconnect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &reconnect_disconnect_headers, b"")
        .await
        .unwrap();
    assert_eq!(reconnect_disconnect.status(), StatusCode::OK);
    assert_eq!(
        reconnect_disconnect
            .headers()
            .get("x-responsecode")
            .unwrap(),
        "0"
    );
                });
        })
        .unwrap()
        .join()
        .unwrap();
}

#[tokio::test]
async fn mapi_over_http_set_search_criteria_accepts_builtin_reminders_refresh() {
    let account = FakeStore::account();
    let search_folders = Arc::new(Mutex::new(vec![SearchFolderDefinition {
        id: Uuid::parse_str("34343434-3434-4434-8434-343434343490").unwrap(),
        account_id: account.account_id,
        role: "reminders".to_string(),
        display_name: "Reminders".to_string(),
        definition_kind: "exchange_builtin".to_string(),
        result_object_kind: "mixed".to_string(),
        scope_json: serde_json::json!({"kind": "builtin"}),
        restriction_json: serde_json::json!({"kind": "builtin"}),
        excluded_folder_roles: Vec::new(),
        is_builtin: true,
    }]));
    let store = FakeStore {
        session: Some(account),
        search_folders: search_folders.clone(),
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

    let mut restriction = vec![0x01];
    restriction.extend_from_slice(&2u16.to_le_bytes());
    append_search_property_bool(&mut restriction, 0x0E69_000B, 0x04, false);
    append_search_property_bool(&mut restriction, 0x0E1B_000B, 0x04, true);
    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, crate::mapi::identity::REMINDERS_FOLDER_ID);
    append_rop_set_search_criteria(
        &mut rops,
        1,
        &restriction,
        &[crate::mapi::identity::IPM_SUBTREE_FOLDER_ID],
        0x0000_0026,
    );
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[1, u32::MAX])),
        )
        .await
        .unwrap();
    let response_rops = response_rops_from_execute_response(response).await;

    assert!(contains_bytes(&response_rops, &[0x30, 0x01, 0, 0, 0, 0]));
    let stored = search_folders.lock().unwrap();
    assert_eq!(stored.len(), 1);
    assert!(stored[0].is_builtin);
    assert_eq!(
        stored[0].restriction_json,
        serde_json::json!({"kind": "builtin"})
    );
}

#[tokio::test]
async fn mapi_over_http_microsoft_oxcdata_reminder_restriction_maps_to_exchange_reminders() {
    let account = FakeStore::account();
    let search_folder_id = Uuid::parse_str("34343434-3434-4434-8434-3434343434a0").unwrap();
    let search_folder_mapi_id = test_mapi_uuid_id(&search_folder_id);
    crate::mapi::identity::remember_mapi_identity(search_folder_id, search_folder_mapi_id);
    let search_folders = Arc::new(Mutex::new(vec![SearchFolderDefinition {
        id: search_folder_id,
        account_id: account.account_id,
        role: "custom".to_string(),
        display_name: "Reminders Search".to_string(),
        definition_kind: "user_saved".to_string(),
        result_object_kind: "mixed".to_string(),
        scope_json: serde_json::json!({}),
        restriction_json: serde_json::json!({}),
        excluded_folder_roles: exchange_reminder_excluded_folder_roles(),
        is_builtin: false,
    }]));
    let store = FakeStore {
        session: Some(account.clone()),
        search_folders: search_folders.clone(),
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

    let excluded_parent_folders = [
        crate::mapi::identity::TRASH_FOLDER_ID,
        crate::mapi::identity::JUNK_FOLDER_ID,
        crate::mapi::identity::DRAFTS_FOLDER_ID,
        crate::mapi::identity::OUTBOX_FOLDER_ID,
        crate::mapi::identity::CONFLICTS_FOLDER_ID,
        crate::mapi::identity::LOCAL_FAILURES_FOLDER_ID,
        crate::mapi::identity::SERVER_FAILURES_FOLDER_ID,
        crate::mapi::identity::SYNC_ISSUES_FOLDER_ID,
    ];
    let mut restriction = vec![0x00];
    restriction.extend_from_slice(&2u16.to_le_bytes());
    restriction.push(0x00);
    restriction.extend_from_slice(&(excluded_parent_folders.len() as u16).to_le_bytes());
    for folder_id in excluded_parent_folders {
        let entry_id =
            crate::mapi::identity::folder_entry_id_from_object_id(account.account_id, folder_id)
                .unwrap();
        append_search_property_binary(&mut restriction, 0x0E09_0102, 0x05, &entry_id);
    }
    restriction.push(0x00);
    restriction.extend_from_slice(&3u16.to_le_bytes());
    restriction.extend_from_slice(&[0x02, 0x00]);
    restriction.extend_from_slice(&2u16.to_le_bytes());
    append_search_exists(&mut restriction, 0x001A_001F);
    restriction.push(0x03);
    restriction.extend_from_slice(&0x0002u16.to_le_bytes());
    restriction.extend_from_slice(&0u16.to_le_bytes());
    restriction.extend_from_slice(&0x001A_001Fu32.to_le_bytes());
    append_mapi_utf16_property(&mut restriction, 0x001A_001F, "IPM.Schedule");
    append_search_bitmask(&mut restriction, 0x0E07_0003, false, 0x0000_0004);
    restriction.push(0x01);
    restriction.extend_from_slice(&2u16.to_le_bytes());
    append_search_property_bool(&mut restriction, 0x8503_000B, 0x04, true);
    restriction.push(0x00);
    restriction.extend_from_slice(&2u16.to_le_bytes());
    append_search_exists(&mut restriction, 0x8223_000B);
    append_search_property_bool(&mut restriction, 0x8223_000B, 0x04, true);

    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, search_folder_mapi_id);
    append_rop_set_search_criteria(
        &mut rops,
        1,
        &restriction,
        &[
            crate::mapi::identity::CALENDAR_FOLDER_ID,
            crate::mapi::identity::TASKS_FOLDER_ID,
        ],
        0x0000_0005,
    );
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[1, u32::MAX])),
        )
        .await
        .unwrap();
    let response_rops = response_rops_from_execute_response(response).await;

    assert!(contains_bytes(&response_rops, &[0x30, 0x01, 0, 0, 0, 0]));
    let stored = search_folders.lock().unwrap();
    assert_eq!(
        stored[0].restriction_json,
        serde_json::json!({
            "kind": "exchange_reminders",
            "match": "reminder_set_or_recurring",
            "recurrenceHorizonDays": 90,
            "occurrenceDismissals": true
        })
    );
    assert_eq!(
        stored[0].scope_json,
        serde_json::json!({
            "kind": "mapi_bounded",
            "scope": "folders",
            "recursive": true,
            "folderIds": [],
            "folderRoles": ["calendar", "tasks"]
        })
    );
}

#[tokio::test]
async fn mapi_over_http_outlook_startup_calendar_folder_chain_uses_advertised_default_calendar() {
    let account = FakeStore::account();
    let store = FakeStore {
        session: Some(account.clone()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = mapi_cookie_header(&connect);

    let calendar_entry_id = crate::mapi::identity::folder_entry_id_from_object_id(
        account.account_id,
        crate::mapi::identity::CALENDAR_FOLDER_ID,
    )
    .unwrap();
    let calendar_long_term_id = crate::mapi::identity::long_term_id_from_object_id(
        crate::mapi::identity::CALENDAR_FOLDER_ID,
    )
    .unwrap();

    let mut rops = Vec::new();
    rops.extend_from_slice(&[0x27, 0x00, 0x00]); // RopGetReceiveFolder.
    rops.extend_from_slice(b"IPM.Appointment.Custom\0");
    rops.extend_from_slice(&[0x26, 0x00, 0x00]); // RopSetReceiveFolder.
    append_mapi_wire_id(&mut rops, crate::mapi::identity::CALENDAR_FOLDER_ID);
    rops.extend_from_slice(b"IPM.Appointment.Custom\0");
    append_rop_get_properties_specific(&mut rops, 0, &[0x36D0_0102]);
    rops.extend_from_slice(&[0x44, 0x00, 0x00]); // RopIdFromLongTermId.
    rops.extend_from_slice(&calendar_long_term_id);
    append_rop_open_folder(&mut rops, 0, 1, crate::mapi::identity::CALENDAR_FOLDER_ID);
    append_rop_get_properties_specific(&mut rops, 1, &[0x3001_001F, 0x3613_001F, 0x65E0_0102]);

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[1, u32::MAX])),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(
        &response_rops,
        &mapi_wire_id_bytes(crate::mapi::identity::CALENDAR_FOLDER_ID)
    ));
    assert!(contains_bytes(&response_rops, b"IPM.Appointment\0"));
    assert!(contains_bytes(&response_rops, &[0x26, 0x00, 0, 0, 0, 0]));
    assert!(contains_bytes(&response_rops, &calendar_entry_id));
    let mut id_from_long_term_response = vec![0x44, 0x00, 0, 0, 0, 0];
    id_from_long_term_response.extend_from_slice(&mapi_wire_id_bytes(
        crate::mapi::identity::CALENDAR_FOLDER_ID,
    ));
    assert!(contains_bytes(&response_rops, &id_from_long_term_response));
    assert!(contains_bytes(
        &response_rops,
        &[0x02, 0x01, 0, 0, 0, 0, 0, 0]
    ));
    assert!(contains_bytes(&response_rops, &utf16z("Calendar")));
    assert!(contains_bytes(&response_rops, &utf16z("IPF.Appointment")));
    assert!(contains_bytes(
        &response_rops,
        &mapi_mailstore::source_key_for_store_id(crate::mapi::identity::CALENDAR_FOLDER_ID)
    ));
}

#[tokio::test]
async fn mapi_over_http_ms_oxosfld_calendar_lookup_chain_opens_calendar_from_inbox() {
    let account = FakeStore::account();
    let store = FakeStore {
        session: Some(account.clone()),
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

    let calendar_entry_id = crate::mapi::identity::folder_entry_id_from_object_id(
        account.account_id,
        crate::mapi::identity::CALENDAR_FOLDER_ID,
    )
    .unwrap();
    let calendar_long_term_id = crate::mapi::identity::long_term_id_from_object_id(
        crate::mapi::identity::CALENDAR_FOLDER_ID,
    )
    .unwrap();

    let mut rops = Vec::new();
    rops.extend_from_slice(&[0x27, 0x00, 0x00]); // RopGetReceiveFolder.
    rops.extend_from_slice(b"IPM\0");
    append_rop_open_folder(&mut rops, 0, 1, crate::mapi::identity::INBOX_FOLDER_ID);
    append_rop_get_properties_specific(&mut rops, 1, &[0x36D0_0102]);
    rops.extend_from_slice(&[0x44, 0x00, 0x00]); // RopIdFromLongTermId.
    rops.extend_from_slice(&calendar_long_term_id);
    append_rop_open_folder(&mut rops, 0, 2, crate::mapi::identity::CALENDAR_FOLDER_ID);
    append_rop_get_properties_specific(&mut rops, 2, &[0x3001_001F, 0x3613_001F, 0x65E0_0102]);

    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    let response_rops = response_rops_from_execute_response(response).await;
    let mut inbox_receive_folder_response = vec![0x27, 0x00, 0, 0, 0, 0];
    append_mapi_wire_id(
        &mut inbox_receive_folder_response,
        crate::mapi::identity::INBOX_FOLDER_ID,
    );
    inbox_receive_folder_response.extend_from_slice(b"IPM\0");
    assert!(contains_bytes(
        &response_rops,
        &inbox_receive_folder_response
    ));
    assert!(contains_bytes(&response_rops, &calendar_entry_id));
    let mut id_from_long_term_response = vec![0x44, 0x00, 0, 0, 0, 0];
    append_mapi_wire_id(
        &mut id_from_long_term_response,
        crate::mapi::identity::CALENDAR_FOLDER_ID,
    );
    assert!(contains_bytes(&response_rops, &id_from_long_term_response));
    assert!(contains_bytes(&response_rops, &utf16z("Calendar")));
    assert!(contains_bytes(&response_rops, &utf16z("IPF.Appointment")));
    assert!(contains_bytes(
        &response_rops,
        &mapi_mailstore::source_key_for_store_id(crate::mapi::identity::CALENDAR_FOLDER_ID)
    ));
}

#[tokio::test]
async fn mapi_over_http_calendar_folder_open_projects_entry_id_identity() {
    let account = FakeStore::account();
    let store = FakeStore {
        session: Some(account.clone()),
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

    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, crate::mapi::identity::CALENDAR_FOLDER_ID);
    append_rop_get_properties_specific(
        &mut rops,
        1,
        &[
            0x0FFF_0102, // PidTagEntryId
            0x0FF6_0102, // PidTagInstanceKey
            0x65E0_0102, // PidTagSourceKey
            0x3001_001F, // PidTagDisplayName
            0x3613_001F, // PidTagContainerClass
            0x36E5_001E, // PidTagDefaultPostMessageClass
            0x36E5_001F, // PidTagDefaultPostMessageClass
        ],
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
    let calendar_entry_id = crate::mapi::identity::folder_entry_id_from_object_id(
        account.account_id,
        crate::mapi::identity::CALENDAR_FOLDER_ID,
    )
    .unwrap();
    assert!(contains_bytes(&response_rops, &calendar_entry_id));
    assert!(contains_bytes(
        &response_rops,
        &mapi_mailstore::source_key_for_store_id(crate::mapi::identity::CALENDAR_FOLDER_ID)
    ));
    assert!(contains_bytes(&response_rops, &utf16z("Calendar")));
    assert!(contains_bytes(&response_rops, &utf16z("IPF.Appointment")));
    assert!(contains_bytes(&response_rops, b"IPM.Appointment\0"));
    assert!(contains_bytes(&response_rops, &utf16z("IPM.Appointment")));
    assert!(!contains_bytes(
        &response_rops,
        &0x8004_0102u32.to_le_bytes()
    ));
}

#[tokio::test]
async fn mapi_over_http_calendar_hierarchy_row_projects_entry_id_identity() {
    let account = FakeStore::account();
    let store = FakeStore {
        session: Some(account.clone()),
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

    let mut rops = Vec::new();
    append_rop_open_folder(
        &mut rops,
        0,
        1,
        crate::mapi::identity::IPM_SUBTREE_FOLDER_ID,
    );
    rops.extend_from_slice(&[0x04, 0x00, 0x01, 0x02, 0x00]); // RopGetHierarchyTable
    rops.extend_from_slice(&[0x12, 0x00, 0x02, 0x00]); // RopSetColumns
    rops.extend_from_slice(&7u16.to_le_bytes());
    rops.extend_from_slice(&0x3001_001Fu32.to_le_bytes());
    rops.extend_from_slice(&0x0FFF_0102u32.to_le_bytes());
    rops.extend_from_slice(&0x0FF6_0102u32.to_le_bytes());
    rops.extend_from_slice(&0x65E0_0102u32.to_le_bytes());
    rops.extend_from_slice(&0x3613_001Fu32.to_le_bytes());
    rops.extend_from_slice(&0x36E5_001Eu32.to_le_bytes());
    rops.extend_from_slice(&0x36E5_001Fu32.to_le_bytes());
    rops.extend_from_slice(&[0x15, 0x00, 0x02, 0x00, 0x01]); // RopQueryRows
    rops.extend_from_slice(&50u16.to_le_bytes());
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
    let calendar_entry_id = crate::mapi::identity::folder_entry_id_from_object_id(
        account.account_id,
        crate::mapi::identity::CALENDAR_FOLDER_ID,
    )
    .unwrap();
    let rows = hierarchy_query_calendar_contract_rows(&response_rops, 8 + 10 + 7)
        .expect("Calendar hierarchy table rows");
    let calendar = rows
        .iter()
        .find(|row| row.display_name == "Calendar")
        .expect("Calendar hierarchy table row");
    assert_eq!(calendar.entry_id, calendar_entry_id);
    assert_eq!(
        calendar.instance_key,
        crate::mapi::identity::instance_key_for_object_id(
            crate::mapi::identity::CALENDAR_FOLDER_ID
        )
    );
    assert_eq!(
        calendar.source_key,
        mapi_mailstore::source_key_for_store_id(crate::mapi::identity::CALENDAR_FOLDER_ID)
    );
    assert_eq!(calendar.container_class, "IPF.Appointment");
    assert_eq!(calendar.default_post_message_class_a, "IPM.Appointment");
    assert_eq!(calendar.default_post_message_class_w, "IPM.Appointment");
    assert!(!contains_bytes(
        &response_rops,
        &0x8004_0102u32.to_le_bytes()
    ));
}

#[tokio::test]
async fn mapi_over_http_custom_calendar_hierarchy_row_projects_owner_entry_id_identity() {
    let account = FakeStore::account();
    let store = FakeStore {
        session: Some(account.clone()),
        calendar_collections: Arc::new(Mutex::new(vec![FakeStore::collection(
            "team-calendar",
            "calendar",
            "Team Calendar",
        )])),
        ..Default::default()
    };
    let snapshot = store
        .load_mapi_mail_store(account.account_id, 100)
        .await
        .unwrap();
    let custom_folder = snapshot
        .collaboration_folders()
        .iter()
        .find(|folder| folder.collection.id == "team-calendar")
        .expect("custom calendar folder projected");
    let custom_folder_id = custom_folder.id;
    let custom_folder_entry_id = crate::mapi::identity::folder_entry_id_from_object_id(
        custom_folder.collection.owner_account_id,
        custom_folder_id,
    )
    .unwrap();
    let nil_folder_entry_id =
        crate::mapi::identity::folder_entry_id_from_object_id(Uuid::nil(), custom_folder_id)
            .unwrap();
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
    append_rop_open_folder(
        &mut rops,
        0,
        1,
        crate::mapi::identity::IPM_SUBTREE_FOLDER_ID,
    );
    rops.extend_from_slice(&[0x04, 0x00, 0x01, 0x02, 0x00]); // RopGetHierarchyTable
    rops.extend_from_slice(&[0x12, 0x00, 0x02, 0x00]); // RopSetColumns
    rops.extend_from_slice(&7u16.to_le_bytes());
    rops.extend_from_slice(&0x3001_001Fu32.to_le_bytes());
    rops.extend_from_slice(&0x0FFF_0102u32.to_le_bytes());
    rops.extend_from_slice(&0x0FF6_0102u32.to_le_bytes());
    rops.extend_from_slice(&0x65E0_0102u32.to_le_bytes());
    rops.extend_from_slice(&0x3613_001Fu32.to_le_bytes());
    rops.extend_from_slice(&0x36E5_001Eu32.to_le_bytes());
    rops.extend_from_slice(&0x36E5_001Fu32.to_le_bytes());
    rops.extend_from_slice(&[0x15, 0x00, 0x02, 0x00, 0x01]); // RopQueryRows
    rops.extend_from_slice(&50u16.to_le_bytes());
    append_rop_open_folder(&mut rops, 0, 3, custom_folder_id);
    append_rop_get_properties_specific(
        &mut rops,
        3,
        &[
            0x0FFF_0102, // PidTagEntryId
            0x0FF6_0102, // PidTagInstanceKey
            0x65E0_0102, // PidTagSourceKey
            0x3001_001F, // PidTagDisplayName
            0x3613_001F, // PidTagContainerClass
            0x36E5_001E, // PidTagDefaultPostMessageClass
            0x36E5_001F, // PidTagDefaultPostMessageClass
        ],
    );

    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(
                &rops,
                &[1, u32::MAX, u32::MAX, u32::MAX, u32::MAX],
            )),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(&response_rops, &utf16z("Team Calendar")));
    let rows = hierarchy_query_calendar_contract_rows(&response_rops, 8 + 10 + 7)
        .expect("custom Calendar hierarchy table rows");
    let team_calendar = rows
        .iter()
        .find(|row| row.display_name == "Team Calendar")
        .expect("custom Calendar hierarchy table row");
    assert_eq!(team_calendar.entry_id, custom_folder_entry_id);
    assert_eq!(
        team_calendar.instance_key,
        crate::mapi::identity::instance_key_for_object_id(custom_folder_id)
    );
    assert_eq!(
        team_calendar.source_key,
        mapi_mailstore::source_key_for_store_id(custom_folder_id)
    );
    assert!(!contains_bytes(&response_rops, &nil_folder_entry_id));
    assert!(contains_bytes(
        &response_rops,
        &mapi_mailstore::source_key_for_store_id(custom_folder_id)
    ));
    assert_eq!(team_calendar.container_class, "IPF.Appointment");
    assert_eq!(
        team_calendar.default_post_message_class_a,
        "IPM.Appointment"
    );
    assert_eq!(
        team_calendar.default_post_message_class_w,
        "IPM.Appointment"
    );
    assert!(contains_bytes(&response_rops, b"IPM.Appointment\0"));
    assert!(contains_bytes(&response_rops, &utf16z("IPM.Appointment")));
}

#[tokio::test]
async fn mapi_over_http_custom_calendar_hierarchy_sync_projects_owner_entry_id_identity() {
    let account = FakeStore::account();
    let store = FakeStore {
        session: Some(account.clone()),
        calendar_collections: Arc::new(Mutex::new(vec![FakeStore::collection(
            "team-calendar",
            "calendar",
            "Team Calendar",
        )])),
        ..Default::default()
    };
    let snapshot = store
        .load_mapi_mail_store(account.account_id, 100)
        .await
        .unwrap();
    let custom_folder = snapshot
        .collaboration_folders()
        .iter()
        .find(|folder| folder.collection.id == "team-calendar")
        .expect("custom calendar folder projected");
    let custom_folder_id = custom_folder.id;
    let custom_folder_entry_id = crate::mapi::identity::folder_entry_id_from_object_id(
        custom_folder.collection.owner_account_id,
        custom_folder_id,
    )
    .unwrap();
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
    append_rop_open_folder(
        &mut rops,
        0,
        1,
        crate::mapi::identity::IPM_SUBTREE_FOLDER_ID,
    );
    append_rop_outlook_hierarchy_sync_manifest_get_buffer(&mut rops, 1, 2, 20000);
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
    let hierarchy =
        strict_hierarchy_sync_transfer_from_response(&response_rops).expect("strict hierarchy ICS");
    let team_calendar = hierarchy
        .folder_changes
        .iter()
        .find(|folder| folder.display_name == "Team Calendar")
        .expect("custom calendar hierarchy sync row");

    assert_eq!(
        team_calendar.container_class.as_deref(),
        Some("IPF.Appointment")
    );
    assert_eq!(
        team_calendar.default_post_message_class.as_deref(),
        Some("IPM.Appointment")
    );
    assert_eq!(team_calendar.folder_id, Some(custom_folder_id));
    assert_eq!(
        team_calendar.parent_folder_id,
        Some(crate::mapi::identity::IPM_SUBTREE_FOLDER_ID)
    );
    assert!(contains_bytes(&response_rops, &custom_folder_entry_id));
    let custom_folder_counter =
        crate::mapi::identity::global_counter_from_store_id(custom_folder_id).unwrap();
    let custom_folder_globcnt = crate::mapi::identity::globcnt_bytes(custom_folder_counter);
    assert!(strict_replguid_globset_contains_counter(
        &hierarchy.idset_given,
        &custom_folder_globcnt
    )
    .unwrap());
    assert!(strict_replguid_globset_contains_counter(
        &hierarchy.cnset_seen,
        &custom_folder_globcnt
    )
    .unwrap());
}

#[tokio::test]
async fn mapi_over_http_shared_calendar_hierarchy_sync_projects_owner_entry_id_identity() {
    let account = FakeStore::account();
    let owner_account_id = Uuid::parse_str("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb").unwrap();
    let mut shared_calendar =
        FakeStore::collection("shared-calendar", "calendar", "Shared Calendar");
    shared_calendar.owner_account_id = owner_account_id;
    shared_calendar.owner_email = "owner@example.test".to_string();
    shared_calendar.owner_display_name = "Owner".to_string();
    shared_calendar.is_owned = false;
    let store = FakeStore {
        session: Some(account.clone()),
        calendar_collections: Arc::new(Mutex::new(vec![shared_calendar])),
        ..Default::default()
    };
    let snapshot = store
        .load_mapi_mail_store(account.account_id, 100)
        .await
        .unwrap();
    let shared_folder = snapshot
        .collaboration_folders()
        .iter()
        .find(|folder| folder.collection.id == "shared-calendar")
        .expect("shared calendar folder projected");
    let shared_folder_id = shared_folder.id;
    let owner_entry_id =
        crate::mapi::identity::folder_entry_id_from_object_id(owner_account_id, shared_folder_id)
            .unwrap();
    let principal_entry_id =
        crate::mapi::identity::folder_entry_id_from_object_id(account.account_id, shared_folder_id)
            .unwrap();
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
    append_rop_open_folder(
        &mut rops,
        0,
        1,
        crate::mapi::identity::IPM_SUBTREE_FOLDER_ID,
    );
    append_rop_outlook_hierarchy_sync_manifest_get_buffer(&mut rops, 1, 2, 20000);
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
    let hierarchy =
        strict_hierarchy_sync_transfer_from_response(&response_rops).expect("strict hierarchy ICS");
    let shared_calendar = hierarchy
        .folder_changes
        .iter()
        .find(|folder| folder.display_name == "Shared Calendar")
        .expect("shared calendar hierarchy sync row");

    assert_eq!(
        shared_calendar.container_class.as_deref(),
        Some("IPF.Appointment")
    );
    assert_eq!(
        shared_calendar.default_post_message_class.as_deref(),
        Some("IPM.Appointment")
    );
    assert_eq!(shared_calendar.folder_id, Some(shared_folder_id));
    assert!(contains_bytes(&response_rops, &owner_entry_id));
    assert!(!contains_bytes(&response_rops, &principal_entry_id));
}

#[tokio::test]
async fn mapi_over_http_shared_calendar_read_only_rights_reject_mutations() {
    let account = FakeStore::account();
    let owner_account_id = Uuid::parse_str("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb").unwrap();
    let event_id = Uuid::parse_str("cccccccc-cccc-4ccc-8ccc-cccccccccccc").unwrap();
    let attachment_id = Uuid::parse_str("dddddddd-dddd-4ddd-8ddd-dddddddddddd").unwrap();
    let mut shared_calendar =
        FakeStore::collection("shared-readonly-calendar", "calendar", "Shared Readonly");
    shared_calendar.owner_account_id = owner_account_id;
    shared_calendar.owner_email = "owner@example.test".to_string();
    shared_calendar.owner_display_name = "Owner".to_string();
    shared_calendar.is_owned = false;
    shared_calendar.rights.may_read = true;
    shared_calendar.rights.may_write = false;
    shared_calendar.rights.may_delete = false;
    shared_calendar.rights.may_share = false;
    let shared_event = AccessibleEvent {
        id: event_id,
        uid: "shared-readonly-event".to_string(),
        collection_id: shared_calendar.id.clone(),
        owner_account_id,
        owner_email: shared_calendar.owner_email.clone(),
        owner_display_name: shared_calendar.owner_display_name.clone(),
        rights: shared_calendar.rights.clone(),
        date: "2026-06-09".to_string(),
        time: "10:00".to_string(),
        time_zone: "UTC".to_string(),
        duration_minutes: 30,
        all_day: false,
        status: "confirmed".to_string(),
        sequence: 0,
        recurrence_rule: String::new(),
        recurrence_json: "{}".to_string(),
        recurrence_exceptions_json: "[]".to_string(),
        title: "Shared readonly before".to_string(),
        location: "Room 900".to_string(),
        organizer_json: "{}".to_string(),
        attendees: String::new(),
        attendees_json: "{}".to_string(),
        notes: String::new(),
        body_html: String::new(),
    };
    let store = FakeStore {
        session: Some(account.clone()),
        calendar_collections: Arc::new(Mutex::new(vec![shared_calendar.clone()])),
        events: Arc::new(Mutex::new(vec![shared_event])),
        calendar_attachments: Arc::new(Mutex::new(HashMap::from([(
            event_id,
            vec![CalendarEventAttachment {
                id: attachment_id,
                event_id,
                file_reference: format!("calendar-attachment:{event_id}:{attachment_id}"),
                file_name: "readonly-agenda.pdf".to_string(),
                media_type: "application/pdf".to_string(),
                size_octets: 17,
            }],
        )]))),
        ..Default::default()
    };
    let snapshot = store
        .load_mapi_mail_store(account.account_id, 100)
        .await
        .unwrap();
    let shared_folder = snapshot
        .collaboration_folders()
        .iter()
        .find(|folder| folder.collection.id == shared_calendar.id)
        .expect("shared calendar folder projected");
    let shared_folder_id = shared_folder.id;
    let mapi_event = snapshot
        .events_for_folder(shared_folder_id)
        .into_iter()
        .find(|candidate| candidate.canonical_id == event_id)
        .expect("shared event projected");
    let service = ExchangeService::new(store.clone());
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
    append_rop_create_message(&mut rops, 0, 1, shared_folder_id);
    append_rop_open_folder(&mut rops, 0, 2, shared_folder_id);
    append_rop_open_message(&mut rops, 2, 3, shared_folder_id, mapi_event.id);
    rops.extend_from_slice(&[0x1E, 0x00, 0x02, 0x00, 0x00]); // RopDeleteMessages.
    rops.extend_from_slice(&1u16.to_le_bytes());
    append_mapi_wire_id(&mut rops, mapi_event.id);

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
    assert!(contains_bytes(
        &response_rops,
        &[0x06, 0x01, 0x05, 0x00, 0x07, 0x80]
    ));
    assert!(contains_bytes(
        &response_rops,
        &[0x1E, 0x02, 0x05, 0x00, 0x07, 0x80]
    ));
    let events = store.events.lock().unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].title, "Shared readonly before");
    let attachments = store.calendar_attachments.lock().unwrap();
    assert_eq!(attachments[&event_id].len(), 1);
    assert_eq!(attachments[&event_id][0].file_name, "readonly-agenda.pdf");
}

#[tokio::test]
async fn mapi_over_http_calendar_get_properties_all_lists_entry_id_identity() {
    let account = FakeStore::account();
    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, crate::mapi::identity::CALENDAR_FOLDER_ID);
    rops.extend_from_slice(&[0x08, 0x00, 0x01, 0x00, 0x10, 0x01, 0x00]); // RopGetPropertiesAll

    let response_rops = execute_rops_response_rops(&rops, &[1, u32::MAX]).await;
    let calendar_entry_id = crate::mapi::identity::folder_entry_id_from_object_id(
        account.account_id,
        crate::mapi::identity::CALENDAR_FOLDER_ID,
    )
    .unwrap();

    assert!(contains_bytes(&response_rops, &[0x08, 0x01, 0, 0, 0, 0]));
    assert!(contains_bytes(
        &response_rops,
        &0x0FFF_0102u32.to_le_bytes()
    ));
    assert!(contains_bytes(
        &response_rops,
        &0x0FF6_0102u32.to_le_bytes()
    ));
    assert!(contains_bytes(
        &response_rops,
        &0x36E5_001Fu32.to_le_bytes()
    ));
    assert!(contains_bytes(&response_rops, &calendar_entry_id));
    assert!(contains_bytes(&response_rops, &utf16z("IPM.Appointment")));
}

#[tokio::test]
async fn mapi_over_http_calendar_get_properties_list_advertises_entry_id_identity() {
    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, crate::mapi::identity::CALENDAR_FOLDER_ID);
    rops.extend_from_slice(&[0x09, 0x00, 0x01]); // RopGetPropertiesList

    let response_rops = execute_rops_response_rops(&rops, &[1, u32::MAX]).await;

    assert!(contains_bytes(&response_rops, &[0x09, 0x01, 0, 0, 0, 0]));
    assert!(contains_bytes(
        &response_rops,
        &0x0FFF_0102u32.to_le_bytes()
    ));
    assert!(contains_bytes(
        &response_rops,
        &0x0FF6_0102u32.to_le_bytes()
    ));
    assert!(contains_bytes(
        &response_rops,
        &0x36E5_001Fu32.to_le_bytes()
    ));
}

#[tokio::test]
async fn mapi_over_http_set_receive_folder_accepts_canonical_calendar_mapping() {
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

    let mut rops = vec![0x26, 0x00, 0x00];
    append_mapi_wire_id(&mut rops, crate::mapi::identity::CALENDAR_FOLDER_ID);
    rops.extend_from_slice(b"IPM.Appointment\0");

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

    assert_eq!(response_rops[0], 0x26);
    assert_eq!(response_rops[1], 0x00);
    assert_eq!(
        u32::from_le_bytes(response_rops[2..6].try_into().unwrap()),
        0
    );
}

#[tokio::test]
async fn mapi_over_http_set_receive_folder_accepts_canonical_custom_calendar_mapping() {
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

    let mut rops = vec![0x26, 0x00, 0x00];
    append_mapi_wire_id(&mut rops, crate::mapi::identity::CALENDAR_FOLDER_ID);
    rops.extend_from_slice(b"IPM.Appointment.Custom\0");

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&rops, &[1]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    let response_rops = response_rops_from_execute_response(response).await;
    assert_eq!(response_rops[0], 0x26);
    assert_eq!(response_rops[1], 0x00);
    assert_eq!(
        u32::from_le_bytes(response_rops[2..6].try_into().unwrap()),
        0
    );
}

#[tokio::test]
async fn mapi_over_http_set_receive_folder_rejects_noncanonical_calendar_mapping() {
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

    let mut rops = vec![0x26, 0x00, 0x00];
    append_mapi_wire_id(&mut rops, crate::mapi::identity::INBOX_FOLDER_ID);
    rops.extend_from_slice(b"IPM.Appointment\0");

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

    assert_eq!(response_rops[0], 0x26);
    assert_eq!(response_rops[1], 0x00);
    assert_eq!(
        u32::from_le_bytes(response_rops[2..6].try_into().unwrap()),
        0x8007_0057
    );
}

#[tokio::test]
async fn mapi_over_http_get_receive_folder_maps_appointments_to_calendar() {
    let mut rops = vec![0x27, 0x00, 0x00];
    rops.extend_from_slice(b"IPM.Appointment\0");

    let response_rops = execute_rops_response_rops(&rops, &[1]).await;

    assert_eq!(&response_rops[..6], &[0x27, 0x00, 0, 0, 0, 0]);
    assert_eq!(
        &response_rops[6..14],
        &mapi_wire_id_bytes(crate::mapi::identity::CALENDAR_FOLDER_ID)
    );
    assert_eq!(&response_rops[14..], b"IPM.Appointment\0");
}

#[tokio::test]
async fn mapi_over_http_store_get_properties_all_lists_calendar_default_entry_id() {
    let mut rops = vec![0xFE, 0x00, 0x00, 0x01]; // Private-mailbox RopLogon.
    let legacy_dn = format!(
        "/o=LPE/ou=Exchange Administrative Group/cn=Recipients/cn={}\0",
        FakeStore::account().email
    );
    rops.extend_from_slice(&0x0100_0004u32.to_le_bytes());
    rops.extend_from_slice(&0u32.to_le_bytes());
    rops.extend_from_slice(&(legacy_dn.len() as u16).to_le_bytes());
    rops.extend_from_slice(legacy_dn.as_bytes());
    rops.extend_from_slice(&[0x08, 0x00, 0x00, 0x00, 0x10, 0x01, 0x00]);

    let response_rops = execute_rops_response_rops(&rops, &[u32::MAX, u32::MAX]).await;

    assert!(contains_bytes(&response_rops, &[0x08, 0x00, 0, 0, 0, 0]));
    assert!(contains_bytes(
        &response_rops,
        &0x36D0_0102u32.to_le_bytes()
    ));
    assert!(contains_bytes(
        &response_rops,
        &crate::mapi::identity::long_term_id_from_object_id(
            crate::mapi::identity::CALENDAR_FOLDER_ID,
        )
        .unwrap(),
    ));
}

#[tokio::test]
async fn mapi_over_http_store_get_properties_list_advertises_calendar_default_entry_id() {
    let mut rops = vec![0xFE, 0x00, 0x00, 0x01]; // Private-mailbox RopLogon.
    let legacy_dn = format!(
        "/o=LPE/ou=Exchange Administrative Group/cn=Recipients/cn={}\0",
        FakeStore::account().email
    );
    rops.extend_from_slice(&0x0100_0004u32.to_le_bytes());
    rops.extend_from_slice(&0u32.to_le_bytes());
    rops.extend_from_slice(&(legacy_dn.len() as u16).to_le_bytes());
    rops.extend_from_slice(legacy_dn.as_bytes());
    rops.extend_from_slice(&[0x09, 0x00, 0x00]); // RopGetPropertiesList on logon.

    let response_rops = execute_rops_response_rops(&rops, &[u32::MAX, u32::MAX]).await;

    assert!(contains_bytes(&response_rops, &[0x09, 0x00, 0, 0, 0, 0]));
    assert!(contains_bytes(
        &response_rops,
        &0x36D0_0102u32.to_le_bytes()
    ));
}

#[tokio::test]
async fn mapi_over_http_store_get_properties_specific_returns_calendar_default_entry_id() {
    let mut rops = vec![0xFE, 0x00, 0x00, 0x01]; // Private-mailbox RopLogon.
    let legacy_dn = format!(
        "/o=LPE/ou=Exchange Administrative Group/cn=Recipients/cn={}\0",
        FakeStore::account().email
    );
    rops.extend_from_slice(&0x0100_0004u32.to_le_bytes());
    rops.extend_from_slice(&0u32.to_le_bytes());
    rops.extend_from_slice(&(legacy_dn.len() as u16).to_le_bytes());
    rops.extend_from_slice(legacy_dn.as_bytes());
    append_rop_get_properties_specific(&mut rops, 1, &[0x36D0_0102]);

    let response_rops = execute_rops_response_rops(&rops, &[u32::MAX, u32::MAX]).await;

    assert!(contains_bytes(&response_rops, &[0x07, 0x01, 0, 0, 0, 0]));
    mapi_get_properties_specific_standard_row_offset(&response_rops, 1)
        .expect("store Calendar default EntryID GetProps should return a standard row");
    assert!(contains_bytes(
        &response_rops,
        &crate::mapi::identity::long_term_id_from_object_id(
            crate::mapi::identity::CALENDAR_FOLDER_ID,
        )
        .unwrap(),
    ));
}

#[tokio::test]
async fn mapi_over_http_root_get_properties_all_lists_calendar_default_entry_id() {
    let mut rops = vec![0x02, 0x00, 0x00, 0x01]; // RopOpenFolder Root.
    append_mapi_wire_id(&mut rops, crate::mapi::identity::ROOT_FOLDER_ID);
    rops.push(0);
    rops.extend_from_slice(&[0x08, 0x00, 0x01, 0x00, 0x10, 0x01, 0x00]);

    let response_rops = execute_rops_response_rops(&rops, &[1, u32::MAX]).await;

    assert!(contains_bytes(&response_rops, &[0x08, 0x01, 0, 0, 0, 0]));
    assert!(contains_bytes(
        &response_rops,
        &0x36D0_0102u32.to_le_bytes()
    ));
    assert!(contains_bytes(
        &response_rops,
        &crate::mapi::identity::long_term_id_from_object_id(
            crate::mapi::identity::CALENDAR_FOLDER_ID,
        )
        .unwrap(),
    ));
}

#[tokio::test]
async fn mapi_over_http_root_get_properties_list_advertises_calendar_default_entry_id() {
    let mut rops = vec![0x02, 0x00, 0x00, 0x01]; // RopOpenFolder Root.
    append_mapi_wire_id(&mut rops, crate::mapi::identity::ROOT_FOLDER_ID);
    rops.push(0);
    rops.extend_from_slice(&[0x09, 0x00, 0x01]); // RopGetPropertiesList on Root.

    let response_rops = execute_rops_response_rops(&rops, &[1, u32::MAX]).await;

    assert!(contains_bytes(&response_rops, &[0x09, 0x01, 0, 0, 0, 0]));
    assert!(contains_bytes(
        &response_rops,
        &0x36D0_0102u32.to_le_bytes()
    ));
}

#[tokio::test]
async fn mapi_over_http_root_get_properties_specific_returns_calendar_default_entry_id() {
    let mut rops = vec![0x02, 0x00, 0x00, 0x01]; // RopOpenFolder Root.
    append_mapi_wire_id(&mut rops, crate::mapi::identity::ROOT_FOLDER_ID);
    rops.push(0);
    append_rop_get_properties_specific(&mut rops, 1, &[0x36D0_0102]);

    let response_rops = execute_rops_response_rops(&rops, &[1, u32::MAX]).await;

    assert!(contains_bytes(&response_rops, &[0x07, 0x01, 0, 0, 0, 0]));
    mapi_get_properties_specific_standard_row_offset(&response_rops, 1)
        .expect("Root Calendar default EntryID GetProps should return a standard row");
    assert!(contains_bytes(
        &response_rops,
        &crate::mapi::identity::long_term_id_from_object_id(
            crate::mapi::identity::CALENDAR_FOLDER_ID,
        )
        .unwrap(),
    ));
}

#[tokio::test]
async fn mapi_over_http_root_get_properties_specific_returns_collaboration_default_entry_ids() {
    let mut rops = vec![0x02, 0x00, 0x00, 0x01]; // RopOpenFolder Root.
    append_mapi_wire_id(&mut rops, crate::mapi::identity::ROOT_FOLDER_ID);
    rops.push(0);
    append_rop_get_properties_specific(&mut rops, 1, &[0x36D2_0102, 0x36D3_0102, 0x36D4_0102]);

    let response_rops = execute_rops_response_rops(&rops, &[1, u32::MAX]).await;

    assert!(contains_bytes(&response_rops, &[0x07, 0x01, 0, 0, 0, 0]));
    mapi_get_properties_specific_standard_row_offset(&response_rops, 1)
        .expect("Root collaboration default EntryID GetProps should return a standard row");
    for folder_id in [
        crate::mapi::identity::JOURNAL_FOLDER_ID,
        crate::mapi::identity::NOTES_FOLDER_ID,
        crate::mapi::identity::TASKS_FOLDER_ID,
    ] {
        assert!(
            contains_bytes(
                &response_rops,
                &crate::mapi::identity::long_term_id_from_object_id(folder_id).unwrap(),
            ),
            "default entry id for folder 0x{folder_id:016x} should be present"
        );
    }
}

#[tokio::test]
async fn mapi_over_http_inbox_get_properties_all_lists_calendar_default_entry_id() {
    let mut rops = vec![0x02, 0x00, 0x00, 0x01]; // RopOpenFolder Inbox.
    append_mapi_wire_id(&mut rops, crate::mapi::identity::INBOX_FOLDER_ID);
    rops.push(0);
    rops.extend_from_slice(&[0x08, 0x00, 0x01, 0x00, 0x10, 0x01, 0x00]);

    let response_rops = execute_rops_response_rops(&rops, &[1, u32::MAX]).await;

    assert!(contains_bytes(&response_rops, &[0x08, 0x01, 0, 0, 0, 0]));
    assert!(contains_bytes(
        &response_rops,
        &0x36D0_0102u32.to_le_bytes()
    ));
    assert!(contains_bytes(
        &response_rops,
        &crate::mapi::identity::long_term_id_from_object_id(
            crate::mapi::identity::CALENDAR_FOLDER_ID,
        )
        .unwrap(),
    ));
}

#[tokio::test]
async fn mapi_over_http_inbox_get_properties_list_advertises_calendar_default_entry_id() {
    let mut rops = vec![0x02, 0x00, 0x00, 0x01]; // RopOpenFolder Inbox.
    append_mapi_wire_id(&mut rops, crate::mapi::identity::INBOX_FOLDER_ID);
    rops.push(0);
    rops.extend_from_slice(&[0x09, 0x00, 0x01]); // RopGetPropertiesList on Inbox.

    let response_rops = execute_rops_response_rops(&rops, &[1, u32::MAX]).await;

    assert!(contains_bytes(&response_rops, &[0x09, 0x01, 0, 0, 0, 0]));
    assert!(contains_bytes(
        &response_rops,
        &0x36D0_0102u32.to_le_bytes()
    ));
}

#[tokio::test]
async fn mapi_over_http_inbox_get_properties_specific_returns_calendar_default_entry_id() {
    let mut rops = vec![0x02, 0x00, 0x00, 0x01]; // RopOpenFolder Inbox.
    append_mapi_wire_id(&mut rops, crate::mapi::identity::INBOX_FOLDER_ID);
    rops.push(0);
    append_rop_get_properties_specific(&mut rops, 1, &[0x36D0_0102]);

    let response_rops = execute_rops_response_rops(&rops, &[1, u32::MAX]).await;

    assert!(contains_bytes(&response_rops, &[0x07, 0x01, 0, 0, 0, 0]));
    mapi_get_properties_specific_standard_row_offset(&response_rops, 1)
        .expect("Inbox Calendar default EntryID GetProps should return a standard row");
    assert!(contains_bytes(
        &response_rops,
        &crate::mapi::identity::long_term_id_from_object_id(
            crate::mapi::identity::CALENDAR_FOLDER_ID,
        )
        .unwrap(),
    ));
}
