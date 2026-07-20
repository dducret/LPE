use super::*;

mod calendar;
mod calendar_identity_scope;
mod connect;
mod contacts;
mod free_busy;
mod hierarchy;
mod local_replica_ids;
mod logon_profile;
mod notifications;
mod nspi;
mod permissions;
mod properties;
mod public_folders;
mod recoverable_items;
mod save_changes_handles;
mod submission;
mod sync;
mod sync_import_deletes;
mod tables;
mod transport;
mod wlink_properties;

fn exchange_reminder_excluded_folder_roles() -> Vec<String> {
    [
        "trash",
        "junk",
        "drafts",
        "outbox",
        "conflicts",
        "local_failures",
        "server_failures",
        "sync_issues",
    ]
    .into_iter()
    .map(str::to_string)
    .collect()
}

fn test_account_legacy_dn(email: &str) -> String {
    test_legacy_dn(email)
}

fn test_contact_legacy_dn(email: &str, id: &str) -> String {
    test_legacy_dn(&format!("{email}-{id}"))
}

fn test_legacy_dn(source: &str) -> String {
    let legacy_cn = source
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() {
                ch.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect::<String>();
    format!("/o=LPE/ou=Exchange Administrative Group/cn=Recipients/cn={legacy_cn}")
}

fn test_category_id(folder_id: u64, property_tag: u32, value: &str) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in folder_id
        .to_le_bytes()
        .into_iter()
        .chain(property_tag.to_le_bytes())
        .chain(value.as_bytes().iter().copied())
    {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01B3);
    }
    hash | 0x8000_0000_0000_0000
}

fn test_collapse_state(folder_id: u64, row_id: u64, position: u32, category_id: u64) -> Vec<u8> {
    let mut state = Vec::new();
    state.extend_from_slice(b"LPECS1");
    state.extend_from_slice(&folder_id.to_le_bytes());
    state.extend_from_slice(&row_id.to_le_bytes());
    state.extend_from_slice(&0u32.to_le_bytes());
    state.extend_from_slice(&position.to_le_bytes());
    state.extend_from_slice(&1u16.to_le_bytes());
    state.extend_from_slice(&1u16.to_le_bytes());
    state.extend_from_slice(&1u16.to_le_bytes());
    state.extend_from_slice(&category_id.to_le_bytes());
    state
}

fn test_daily_calendar_recur_blob() -> Vec<u8> {
    let mut value = Vec::new();
    value.extend_from_slice(&0x3004u16.to_le_bytes());
    value.extend_from_slice(&0x3004u16.to_le_bytes());
    value.extend_from_slice(&0x200Au16.to_le_bytes());
    value.extend_from_slice(&0u16.to_le_bytes());
    value.extend_from_slice(&0u16.to_le_bytes());
    value.extend_from_slice(&223701060u32.to_le_bytes());
    value.extend_from_slice(&(1440u32).to_le_bytes());
    value.extend_from_slice(&0u32.to_le_bytes());
    value.extend_from_slice(&0x0000_2022u32.to_le_bytes());
    value.extend_from_slice(&3u32.to_le_bytes());
    value.extend_from_slice(&0u32.to_le_bytes());
    value.extend_from_slice(&0u32.to_le_bytes());
    value.extend_from_slice(&0u32.to_le_bytes());
    value.extend_from_slice(&223729860u32.to_le_bytes());
    value.extend_from_slice(&223735620u32.to_le_bytes());
    value.extend_from_slice(&0x0000_3006u32.to_le_bytes());
    value.extend_from_slice(&0x0000_3009u32.to_le_bytes());
    value.extend_from_slice(&(9 * 60u32).to_le_bytes());
    value.extend_from_slice(&(10 * 60u32).to_le_bytes());
    value.extend_from_slice(&0u16.to_le_bytes());
    value.extend_from_slice(&0u32.to_le_bytes());
    value.extend_from_slice(&0u32.to_le_bytes());
    value
}

fn test_calendar_time_zone_definition(key_name: &str) -> Vec<u8> {
    let key_name = key_name.encode_utf16().collect::<Vec<_>>();
    let cb_header = 2usize
        .saturating_add(2)
        .saturating_add(key_name.len().saturating_mul(2))
        .saturating_add(2) as u16;
    let mut value = vec![0x02, 0x01];
    value.extend_from_slice(&cb_header.to_le_bytes());
    value.extend_from_slice(&0x0002u16.to_le_bytes());
    value.extend_from_slice(&(key_name.len() as u16).to_le_bytes());
    for unit in key_name {
        value.extend_from_slice(&unit.to_le_bytes());
    }
    value.extend_from_slice(&1u16.to_le_bytes());
    value.extend_from_slice(&[0x02, 0x01]);
    value.extend_from_slice(&0x003Eu16.to_le_bytes());
    value.extend_from_slice(&0x0002u16.to_le_bytes());
    value.extend_from_slice(&0u16.to_le_bytes());
    value.extend_from_slice(&[0; 14]);
    value.extend_from_slice(&(-60i32).to_le_bytes());
    value.extend_from_slice(&0i32.to_le_bytes());
    value.extend_from_slice(&(-60i32).to_le_bytes());
    value.extend_from_slice(&[0, 0, 10, 0, 0, 0, 5, 0, 3, 0, 0, 0, 0, 0, 0, 0]);
    value.extend_from_slice(&[0, 0, 3, 0, 0, 0, 5, 0, 2, 0, 0, 0, 0, 0, 0, 0]);
    value
}

fn open_embedded_message_response_contains_subject(response_rops: &[u8], subject: &str) -> bool {
    let mut tail = vec![0x01, 0x04];
    tail.extend_from_slice(&utf16z(subject));
    let success_prefix = [0x46, 0x04, 0, 0, 0, 0, 0];
    response_rops
        .windows(success_prefix.len())
        .enumerate()
        .filter(|(_, window)| *window == success_prefix)
        .any(|(offset, _)| {
            let tail_offset = offset + success_prefix.len() + 8 + 1;
            response_rops
                .get(tail_offset..tail_offset + tail.len())
                .is_some_and(|candidate| candidate == tail)
        })
}

#[tokio::test(flavor = "current_thread")]
async fn mapi_over_http_set_properties_updates_canonical_event_and_task_reminders() {
    let account = FakeStore::account();
    let calendar = FakeStore::collection("default", "calendar", "Calendar");
    let task_list = FakeStore::collection("default", "task", "Tasks");
    let event_id = Uuid::parse_str("31313131-3131-3131-3131-313131313131").unwrap();
    let task_id = Uuid::parse_str("32323232-3232-3232-3232-323232323232").unwrap();
    let reminders = Arc::new(Mutex::new(vec![ClientReminder {
        source_type: "task".to_string(),
        source_id: task_id,
        occurrence_start_at: None,
        title: "Task reminder source".to_string(),
        due_at: Some("2026-05-21T12:00:00Z".to_string()),
        reminder_at: "2026-05-21T11:30:00Z".to_string(),
        dismissed_at: None,
        completed_at: None,
        status: "pending".to_string(),
    }]));
    let store = FakeStore {
        session: Some(account.clone()),
        calendar_collections: Arc::new(Mutex::new(vec![calendar.clone()])),
        task_collections: Arc::new(Mutex::new(vec![task_list.clone()])),
        events: Arc::new(Mutex::new(vec![AccessibleEvent {
            id: event_id,
            uid: event_id.to_string(),
            collection_id: calendar.id,
            owner_account_id: account.account_id,
            owner_email: account.email.clone(),
            owner_display_name: account.display_name.clone(),
            rights: FakeStore::rights(),
            date: "2026-05-21".to_string(),
            time: "09:00".to_string(),
            time_zone: "UTC".to_string(),
            duration_minutes: 30,
            all_day: false,
            status: "confirmed".to_string(),
            sequence: 0,
            recurrence_rule: String::new(),
            recurrence_json: "{}".to_string(),
            recurrence_exceptions_json: "[]".to_string(),
            title: "Calendar reminder source".to_string(),
            location: String::new(),
            organizer_json: "{}".to_string(),
            attendees: String::new(),
            attendees_json: String::new(),
            notes: String::new(),
            body_html: String::new(),
        }])),
        tasks: Arc::new(Mutex::new(vec![FakeStore::task(
            &task_id.to_string(),
            "33333333-3333-3333-3333-333333333333",
            "Task reminder source",
        )])),
        search_folders: Arc::new(Mutex::new(vec![SearchFolderDefinition {
            id: Uuid::parse_str("34343434-3434-3434-3434-343434343434").unwrap(),
            account_id: account.account_id,
            role: "reminders".to_string(),
            display_name: "Reminders".to_string(),
            definition_kind: "exchange_builtin".to_string(),
            result_object_kind: "mixed".to_string(),
            scope_json: serde_json::json!({"scope": "top_of_personal_folders"}),
            restriction_json: serde_json::json!({
                "kind": "exchange_reminders",
                "match": "reminder_set_or_recurring",
                "recurrenceHorizonDays": 90,
                "occurrenceDismissals": true
            }),
            excluded_folder_roles: exchange_reminder_excluded_folder_roles(),
            is_builtin: true,
        }])),
        reminders: reminders.clone(),
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

    let calendar_reminder_at = "2026-05-21T08:45:00Z";
    let task_reminder_at = "2026-05-21T11:45:00Z";
    let mut event_values = Vec::new();
    event_values.extend_from_slice(&0x8503_000Bu32.to_le_bytes());
    event_values.push(1);
    append_mapi_i64_property(
        &mut event_values,
        0x8560_0040,
        mapi_mailstore::filetime_from_rfc3339_utc(calendar_reminder_at) as i64,
    );
    let mut task_values = Vec::new();
    task_values.extend_from_slice(&0x8503_000Bu32.to_le_bytes());
    task_values.push(1);
    append_mapi_i64_property(
        &mut task_values,
        0x8560_0040,
        mapi_mailstore::filetime_from_rfc3339_utc(task_reminder_at) as i64,
    );

    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, crate::mapi::identity::CALENDAR_FOLDER_ID);
    append_rop_open_message_with_flags(
        &mut rops,
        1,
        2,
        crate::mapi::identity::CALENDAR_FOLDER_ID,
        crate::mapi::identity::legacy_migration_object_id(&event_id),
        0x01,
    );
    append_rop_set_properties(&mut rops, 2, 2, &event_values);
    append_rop_save_changes_message(&mut rops, 1, 2);
    append_rop_open_folder(&mut rops, 0, 3, crate::mapi::identity::TASKS_FOLDER_ID);
    append_rop_open_message_with_flags(
        &mut rops,
        3,
        4,
        crate::mapi::identity::TASKS_FOLDER_ID,
        test_mapi_uuid_id(&task_id),
        0x01,
    );
    append_rop_set_properties(&mut rops, 4, 2, &task_values);
    append_rop_save_changes_message(&mut rops, 3, 4);

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", cookie);
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(
                &rops,
                &[u32::MAX, u32::MAX, u32::MAX, u32::MAX, u32::MAX],
            )),
        )
        .await
        .unwrap();
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(
        &response_rops,
        &[0x0A, 0x02, 0, 0, 0, 0, 0, 0]
    ));
    assert!(contains_bytes(
        &response_rops,
        &[0x0A, 0x04, 0, 0, 0, 0, 0, 0]
    ));
    let reminders = reminders.lock().unwrap();
    assert!(reminders.iter().any(|reminder| {
        reminder.source_type == "calendar"
            && reminder.source_id == event_id
            && reminder.reminder_at == calendar_reminder_at
    }));
    assert!(reminders.iter().any(|reminder| {
        reminder.source_type == "task"
            && reminder.source_id == task_id
            && reminder.reminder_at == task_reminder_at
    }));
}

async fn modify_rules_response(
    name: &str,
    provider_data: serde_json::Value,
) -> (Vec<u8>, Option<String>) {
    let inbox_id = "55555555-5555-5555-5555-555555555555";
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            inbox_id, "inbox", "Inbox",
        )])),
        ..Default::default()
    };
    let active_sieve = store.active_sieve_script.clone();
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
    rops.extend_from_slice(&[0x41, 0x00, 0x01, 0x00]);
    rops.extend_from_slice(&1u16.to_le_bytes());
    rops.push(0x01);
    rops.extend_from_slice(&3u16.to_le_bytes());
    append_mapi_utf16_property(&mut rops, 0x6682_001F, name);
    rops.extend_from_slice(&0x6677_0003u32.to_le_bytes());
    rops.extend_from_slice(&1u32.to_le_bytes());
    let provider_data = provider_data.to_string();
    rops.extend_from_slice(&0x6684_0102u32.to_le_bytes());
    rops.extend_from_slice(&(provider_data.len() as u16).to_le_bytes());
    rops.extend_from_slice(provider_data.as_bytes());

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
    let sieve = active_sieve.lock().unwrap().clone();
    (response_rops, sieve)
}

#[tokio::test(flavor = "current_thread")]
async fn mapi_over_http_reminders_folder_open_uses_canonical_search_projection() {
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
    append_mapi_wire_id(&mut rops, crate::mapi::identity::REMINDERS_FOLDER_ID);
    rops.push(0);
    rops.extend_from_slice(&[
        0x07, 0x00, 0x01, // RopGetPropertiesSpecific
    ]);
    rops.extend_from_slice(&4096u16.to_le_bytes());
    rops.extend_from_slice(&2u16.to_le_bytes());
    rops.extend_from_slice(&0x3601_0003u32.to_le_bytes());
    rops.extend_from_slice(&0x3613_001Fu32.to_le_bytes());

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&rops, &[1, u32::MAX]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    let response_rops = response_rops_from_execute_response(response).await;
    assert_eq!(&response_rops[0..8], &[0x02, 0x01, 0, 0, 0, 0, 0, 0]);
    assert!(contains_bytes(&response_rops, &2u32.to_le_bytes()));
    assert!(contains_bytes(&response_rops, &utf16z("Outlook.Reminder")));
}

#[tokio::test(flavor = "current_thread")]
async fn mapi_over_http_root_rem_online_entry_id_projects_reminders_folder() {
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
    append_mapi_wire_id(&mut rops, crate::mapi::identity::ROOT_FOLDER_ID);
    rops.push(0);
    rops.extend_from_slice(&[
        0x07, 0x00, 0x01, // RopGetPropertiesSpecific
    ]);
    rops.extend_from_slice(&4096u16.to_le_bytes());
    rops.extend_from_slice(&1u16.to_le_bytes());
    rops.extend_from_slice(&0x36D5_0102u32.to_le_bytes());

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&rops, &[1, u32::MAX]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    let response_rops = response_rops_from_execute_response(response).await;
    let get_props_offset = 8;
    assert_eq!(response_rops[get_props_offset], 0x07);
    let entry_id = crate::mapi::identity::long_term_id_from_object_id(
        crate::mapi::identity::REMINDERS_FOLDER_ID,
    )
    .unwrap()
    .to_vec();
    assert!(contains_bytes(&response_rops, &entry_id));
}

#[tokio::test(flavor = "current_thread")]
async fn mapi_over_http_reminders_table_projects_canonical_mixed_rows() {
    let account = FakeStore::account();
    let calendar = FakeStore::collection("calendar", "calendar", "Calendar");
    let task_list_id = "22222222-2222-2222-2222-222222222222";
    let task_list = FakeStore::collection(task_list_id, "task", "Tasks");
    let inbox = FakeStore::mailbox("55555555-5555-5555-5555-555555555555", "inbox", "Inbox");
    let event_id = Uuid::parse_str("11111111-1111-1111-1111-111111111111").unwrap();
    let task_id = Uuid::parse_str("33333333-3333-3333-3333-333333333333").unwrap();
    let mail_id = Uuid::parse_str("66666666-6666-6666-6666-666666666666").unwrap();
    let mut email = FakeStore::email(
        &mail_id.to_string(),
        &inbox.id.to_string(),
        "inbox",
        "Mail reminder",
    );
    email.reminder_set = true;
    email.reminder_at = Some("2026-05-21T12:00:00Z".to_string());
    email.mailbox_states[0].reminder_set = true;
    email.mailbox_states[0].reminder_at = email.reminder_at.clone();
    let store = FakeStore {
        session: Some(account.clone()),
        calendar_collections: Arc::new(Mutex::new(vec![calendar.clone()])),
        task_collections: Arc::new(Mutex::new(vec![task_list.clone()])),
        events: Arc::new(Mutex::new(vec![AccessibleEvent {
            id: event_id,
            uid: event_id.to_string(),
            collection_id: calendar.id.clone(),
            owner_account_id: account.account_id,
            owner_email: account.email.clone(),
            owner_display_name: account.display_name.clone(),
            rights: FakeStore::rights(),
            date: "2026-05-21".to_string(),
            time: "09:00".to_string(),
            time_zone: "UTC".to_string(),
            duration_minutes: 30,
            all_day: false,
            status: "confirmed".to_string(),
            sequence: 0,
            recurrence_rule: String::new(),
            recurrence_json: "{}".to_string(),
            recurrence_exceptions_json: "[]".to_string(),
            title: "Calendar reminder".to_string(),
            location: String::new(),
            organizer_json: "{}".to_string(),
            attendees: String::new(),
            attendees_json: String::new(),
            notes: String::new(),
            body_html: String::new(),
        }])),
        tasks: Arc::new(Mutex::new(vec![FakeStore::task(
            &task_id.to_string(),
            task_list_id,
            "Task reminder",
        )])),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
        emails: Arc::new(Mutex::new(vec![email])),
        search_folders: Arc::new(Mutex::new(vec![SearchFolderDefinition {
            id: Uuid::parse_str("77777777-7777-7777-7777-777777777777").unwrap(),
            account_id: account.account_id,
            role: "reminders".to_string(),
            display_name: "Reminders".to_string(),
            definition_kind: "exchange_builtin".to_string(),
            result_object_kind: "mixed".to_string(),
            scope_json: serde_json::json!({"scope": "top_of_personal_folders"}),
            restriction_json: serde_json::json!({"kind": "exchange_reminders"}),
            excluded_folder_roles: exchange_reminder_excluded_folder_roles(),
            is_builtin: true,
        }])),
        reminders: Arc::new(Mutex::new(vec![
            ClientReminder {
                source_type: "calendar".to_string(),
                source_id: event_id,
                occurrence_start_at: None,
                title: "Calendar reminder".to_string(),
                due_at: Some("2026-05-21T09:00:00Z".to_string()),
                reminder_at: "2026-05-21T08:45:00Z".to_string(),
                dismissed_at: None,
                completed_at: None,
                status: "pending".to_string(),
            },
            ClientReminder {
                source_type: "task".to_string(),
                source_id: task_id,
                occurrence_start_at: None,
                title: "Task reminder".to_string(),
                due_at: Some("2026-05-21T12:00:00Z".to_string()),
                reminder_at: "2026-05-21T11:45:00Z".to_string(),
                dismissed_at: None,
                completed_at: None,
                status: "pending".to_string(),
            },
            ClientReminder {
                source_type: "mail".to_string(),
                source_id: mail_id,
                occurrence_start_at: None,
                title: "Mail reminder".to_string(),
                due_at: Some("2026-05-21T12:00:00Z".to_string()),
                reminder_at: "2026-05-21T12:00:00Z".to_string(),
                dismissed_at: None,
                completed_at: None,
                status: "pending".to_string(),
            },
        ])),
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
    append_mapi_wire_id(&mut rops, crate::mapi::identity::REMINDERS_FOLDER_ID);
    rops.push(0);
    rops.extend_from_slice(&[
        0x05, 0x00, 0x01, 0x02, 0x00, // RopGetContentsTable
        0x37, 0x00, 0x02, // RopQueryColumnsAll
        0x12, 0x00, 0x02, 0x00, // RopSetColumns
    ]);
    rops.extend_from_slice(&4u16.to_le_bytes());
    rops.extend_from_slice(&0x0037_001Fu32.to_le_bytes());
    rops.extend_from_slice(&0x001A_001Fu32.to_le_bytes());
    rops.extend_from_slice(&0x674A_0014u32.to_le_bytes());
    rops.extend_from_slice(&0x8503_000Bu32.to_le_bytes());
    rops.extend_from_slice(&[
        0x13, 0x00, 0x02, 0x00, // RopSortTable
    ]);
    rops.extend_from_slice(&1u16.to_le_bytes());
    rops.extend_from_slice(&0u16.to_le_bytes());
    rops.extend_from_slice(&0u16.to_le_bytes());
    rops.extend_from_slice(&0x0037_001Fu32.to_le_bytes());
    rops.push(0);
    rops.extend_from_slice(&[0x15, 0x00, 0x02, 0x00, 0x01]);
    rops.extend_from_slice(&10u16.to_le_bytes());
    append_rop_open_message(
        &mut rops,
        0,
        3,
        crate::mapi::identity::REMINDERS_FOLDER_ID,
        crate::mapi::identity::legacy_migration_object_id(&mail_id),
    );

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX, u32::MAX]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    let response_rops = response_rops_from_execute_response(response).await;
    let contents_offset = 8;
    assert_eq!(response_rops[contents_offset], 0x05);
    assert_eq!(
        u32::from_le_bytes(
            response_rops[contents_offset + 6..contents_offset + 10]
                .try_into()
                .unwrap()
        ),
        2
    );
    assert!(contains_bytes(
        &response_rops,
        &0x8503_000Bu32.to_le_bytes()
    ));
    assert!(contains_bytes(
        &response_rops,
        &0x8502_0040u32.to_le_bytes()
    ));
    assert!(contains_bytes(
        &response_rops,
        &0x8560_0040u32.to_le_bytes()
    ));
    assert!(!contains_bytes(
        &response_rops,
        &0x0C1A_001Fu32.to_le_bytes()
    ));
    assert!(!contains_bytes(
        &response_rops,
        &utf16z("Calendar reminder")
    ));
    assert!(contains_bytes(&response_rops, &utf16z("Task reminder")));
    assert!(contains_bytes(&response_rops, &utf16z("Mail reminder")));
    let mail_name = utf16z("Mail reminder");
    let task_name = utf16z("Task reminder");
    let mail_offset = response_rops
        .windows(mail_name.len())
        .position(|window| window == mail_name.as_slice())
        .unwrap();
    let task_offset = response_rops
        .windows(task_name.len())
        .position(|window| window == task_name.as_slice())
        .unwrap();
    assert!(mail_offset < task_offset);
    assert!(!contains_bytes(&response_rops, &utf16z("IPM.Appointment")));
    assert!(contains_bytes(&response_rops, &utf16z("IPM.Task")));
    assert!(contains_bytes(&response_rops, &utf16z("IPM.Note")));
    assert!(contains_bytes(&response_rops, &[0x03, 0x03, 0, 0, 0, 0]));
}

async fn abort_submit_response(status: &str) -> (Vec<u8>, Vec<Uuid>) {
    let sent_mailbox = FakeStore::mailbox("22222222-2222-2222-2222-222222222222", "sent", "Sent");
    let message_id = Uuid::parse_str("87878787-8787-8787-8787-878787878787").unwrap();
    let mapi_message_id = crate::mapi::identity::legacy_migration_object_id(&message_id);
    crate::mapi::identity::remember_mapi_identity(message_id, mapi_message_id);
    let mut email = FakeStore::email(
        &message_id.to_string(),
        &sent_mailbox.id.to_string(),
        "sent",
        "Abort submit",
    );
    email.delivery_status = status.to_string();
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![sent_mailbox])),
        emails: Arc::new(Mutex::new(vec![email])),
        mapi_identities: Arc::new(Mutex::new(HashMap::from([(message_id, mapi_message_id)]))),
        ..Default::default()
    };
    let cancelled_submissions = store.cancelled_submissions.clone();
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = mapi_cookie_header(&connect);
    let mut rops = Vec::new();
    rops.extend_from_slice(&[0x34, 0x00, 0x00]);
    append_mapi_wire_id(&mut rops, test_mapi_folder_id(7));
    append_mapi_wire_id(&mut rops, mapi_message_id);
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[1])),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    let response_rops = response_rops_from_execute_response(response).await;
    let cancelled = cancelled_submissions.lock().unwrap().clone();
    (response_rops, cancelled)
}
