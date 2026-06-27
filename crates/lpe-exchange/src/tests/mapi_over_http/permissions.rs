use super::*;

#[tokio::test]
async fn mapi_over_http_freebusy_data_folder_projects_canonical_delegate_and_freebusy_messages() {
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
    rops.extend_from_slice(&[0x05, 0x00, 0x01, 0x02, 0x02]); // associated contents table
    rops.extend_from_slice(&[0x12, 0x00, 0x02, 0x00]);
    rops.extend_from_slice(&3u16.to_le_bytes());
    rops.extend_from_slice(&0x0037_001Fu32.to_le_bytes());
    rops.extend_from_slice(&0x001A_001Fu32.to_le_bytes());
    rops.extend_from_slice(&0x6748_0014u32.to_le_bytes());
    rops.extend_from_slice(&[0x15, 0x00, 0x02, 0x00, 0x01]);
    rops.extend_from_slice(&50u16.to_le_bytes());
    append_rop_open_message(
        &mut rops,
        1,
        3,
        crate::mapi::identity::FREEBUSY_DATA_FOLDER_ID,
        test_mapi_uuid_id(&delegate_message_id),
    );
    append_rop_get_properties_specific(&mut rops, 3, &[0x0037_001F, 0x001A_001F, 0x1000_001F]);

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
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
    assert!(
        contains_bytes(&response_rops, &[0x15, 0x02, 0, 0, 0, 0, 0, 2, 0]),
        "{response_rops:02x?}"
    );
    assert!(contains_bytes(
        &response_rops,
        &utf16z("Delegate access for owner@example.test")
    ));
    assert!(contains_bytes(
        &response_rops,
        &utf16z("IPM.Microsoft.Delegate")
    ));
    assert!(contains_bytes(
        &response_rops,
        &utf16z("Free/busy for owner@example.test")
    ));
    assert!(contains_bytes(
        &response_rops,
        &utf16z("IPM.Microsoft.ScheduleData.FreeBusy")
    ));
    assert!(
        contains_bytes(&response_rops, &[0x03, 0x03, 0, 0, 0, 0]),
        "{response_rops:02x?}"
    );
    assert!(contains_bytes(
        &response_rops,
        &utf16z("calendarRead=true; calendarWrite=true")
    ));
}

#[tokio::test]
async fn mapi_over_http_sharing_8aa6_named_property_no_create_is_well_known() {
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
    let psetid_sharing_guid = [
        0x40, 0x20, 0x06, 0x00, 0x00, 0x00, 0x00, 0x00, 0xC0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x46,
    ];

    let mut rops = vec![
        0xFE, 0x00, 0x00, 0x01, // RopLogon
    ];
    rops.extend_from_slice(&0u32.to_le_bytes());
    rops.extend_from_slice(&0u32.to_le_bytes());
    rops.extend_from_slice(&0u16.to_le_bytes());
    rops.extend_from_slice(&[
        0x56, 0x00, 0x00, 0x00, // RopGetPropertyIdsFromNames, do not create missing
    ]);
    rops.extend_from_slice(&1u16.to_le_bytes());
    rops.push(0x00);
    rops.extend_from_slice(&psetid_sharing_guid);
    rops.extend_from_slice(&0x8AA6u32.to_le_bytes());

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[u32::MAX])),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(
        &response_rops,
        &[0x56, 0x00, 0, 0, 0, 0, 1, 0, 0xA6, 0x8A]
    ));
    assert!(!contains_bytes(
        &response_rops,
        &[0x56, 0x00, 0x0f, 0x01, 0x04, 0x80]
    ));
}

#[tokio::test]
async fn mapi_over_http_calendar_modify_permissions_writes_postgresql_calendar_grant(
) -> anyhow::Result<()> {
    let Some(fixture) = postgres_mapi_calendar_fixture().await? else {
        return Ok(());
    };
    let storage = fixture.storage.clone();
    let tenant_id = Uuid::parse_str("10000000-0000-0000-0000-000000000001").unwrap();
    let domain_id = Uuid::parse_str("10000000-0000-0000-0000-000000000002").unwrap();
    let grantee_account_id = Uuid::parse_str("10000000-0000-0000-0000-000000000005").unwrap();
    sqlx::query(
        r#"
        INSERT INTO accounts (id, tenant_id, primary_domain_id, primary_email, display_name)
        VALUES ($1, $2, $3, 'bob@example.test', 'Bob Delegate')
        "#,
    )
    .bind(grantee_account_id)
    .bind(tenant_id)
    .bind(domain_id)
    .execute(storage.pool())
    .await?;
    let identities = storage
        .fetch_or_allocate_mapi_identities(
            fixture.account_id,
            &[MapiIdentityRequest {
                object_kind: MapiIdentityObjectKind::Account,
                canonical_id: grantee_account_id,
                reserved_global_counter: None,
                source_key: None,
            }],
        )
        .await?;
    let delegate_member_id = identities[0].object_id;

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
    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, crate::mapi::identity::CALENDAR_FOLDER_ID);
    rops.extend_from_slice(&[0x40, 0x00, 0x01, 0x00]); // RopModifyPermissions.
    rops.extend_from_slice(&1u16.to_le_bytes());
    rops.push(0x01);
    rops.extend_from_slice(&2u16.to_le_bytes());
    rops.extend_from_slice(&0x6671_0014u32.to_le_bytes());
    rops.extend_from_slice(&(delegate_member_id as i64).to_le_bytes());
    rops.extend_from_slice(&0x6673_0003u32.to_le_bytes());
    rops.extend_from_slice(
        &(crate::mapi::permissions::rights_from_grant(true, true, false, false) as i32)
            .to_le_bytes(),
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
    assert!(contains_bytes(&response_rops, &[0x40, 0x01, 0, 0, 0, 0]));

    let grants = storage
        .fetch_outgoing_collaboration_grants(
            fixture.account_id,
            lpe_storage::CollaborationResourceKind::Calendar,
        )
        .await?;
    let grant = grants
        .iter()
        .find(|grant| grant.grantee_account_id == grantee_account_id)
        .expect("calendar grant was written");
    assert_eq!(grant.owner_account_id, fixture.account_id);
    assert!(grant.rights.may_read);
    assert!(grant.rights.may_write);
    assert!(!grant.rights.may_delete);
    assert!(!grant.rights.may_share);

    let delegate_calendars = storage
        .fetch_accessible_calendar_collections(grantee_account_id)
        .await?;
    assert!(delegate_calendars.iter().any(|calendar| {
        calendar.owner_account_id == fixture.account_id && calendar.rights.may_write
    }));

    fixture.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn mapi_over_http_freebusy_data_sync_projects_postgresql_delegate_state() -> anyhow::Result<()>
{
    let Some(fixture) = postgres_mapi_calendar_fixture().await? else {
        return Ok(());
    };
    let storage = fixture.storage.clone();
    let tenant_id = Uuid::parse_str("10000000-0000-0000-0000-000000000001").unwrap();
    let domain_id = Uuid::parse_str("10000000-0000-0000-0000-000000000002").unwrap();
    let delegate_account_id = Uuid::parse_str("10000000-0000-0000-0000-000000000006").unwrap();
    sqlx::query(
        r#"
        INSERT INTO accounts (id, tenant_id, primary_domain_id, primary_email, display_name)
        VALUES ($1, $2, $3, 'delegate@example.test', 'Delegate User')
        "#,
    )
    .bind(delegate_account_id)
    .bind(tenant_id)
    .bind(domain_id)
    .execute(storage.pool())
    .await?;
    sqlx::query(
        r#"
        INSERT INTO account_sessions (id, tenant_id, token, account_email, expires_at)
        VALUES ($1, $2, 'delegate-token', 'delegate@example.test', NOW() + INTERVAL '1 hour')
        "#,
    )
    .bind(Uuid::parse_str("10000000-0000-0000-0000-000000000007").unwrap())
    .bind(tenant_id)
    .execute(storage.pool())
    .await?;
    storage
        .upsert_client_event(UpsertClientEventInput {
            id: Some(Uuid::parse_str("76767676-7676-4676-9676-767676767676").unwrap()),
            account_id: fixture.account_id,
            uid: "mapi-calendar-freebusy-postgres".to_string(),
            date: "2026-06-09".to_string(),
            time: "09:30".to_string(),
            time_zone: "UTC".to_string(),
            duration_minutes: 60,
            all_day: false,
            status: "confirmed".to_string(),
            sequence: 0,
            recurrence_rule: String::new(),
            recurrence_json: "{}".to_string(),
            recurrence_exceptions_json: "[]".to_string(),
            title: "Canonical busy block".to_string(),
            location: "Room 704".to_string(),
            organizer_json: "{}".to_string(),
            attendees: String::new(),
            attendees_json: "{}".to_string(),
            notes: String::new(),
            body_html: String::new(),
        })
        .await?;
    storage
        .upsert_collaboration_grant(
            CollaborationGrantInput {
                kind: CollaborationResourceKind::Calendar,
                owner_account_id: fixture.account_id,
                grantee_email: "delegate@example.test".to_string(),
                calendar_id: None,
                may_read: true,
                may_write: true,
                may_delete: false,
                may_share: false,
            },
            lpe_storage::AuditEntryInput {
                actor: "alice@example.test".to_string(),
                action: "test-calendar-grant".to_string(),
                subject: "delegate@example.test".to_string(),
            },
        )
        .await?;
    storage
        .upsert_sender_delegation_grant(
            SenderDelegationGrantInput {
                owner_account_id: fixture.account_id,
                grantee_email: "delegate@example.test".to_string(),
                sender_right: SenderDelegationRight::SendOnBehalf,
            },
            lpe_storage::AuditEntryInput {
                actor: "alice@example.test".to_string(),
                action: "test-sender-right".to_string(),
                subject: "delegate@example.test".to_string(),
            },
        )
        .await?;

    let snapshot = storage
        .load_mapi_mail_store(delegate_account_id, 500)
        .await?;
    assert_eq!(snapshot.delegate_freebusy_messages().len(), 2);
    assert!(snapshot.delegate_freebusy_messages().iter().any(|message| {
        message.message.message_kind == "delegate"
            && message
                .message
                .body_text
                .contains("meetingObjects=true; sendOnBehalf=true")
    }));
    assert!(snapshot.delegate_freebusy_messages().iter().any(|message| {
        message.message.message_kind == "freebusy"
            && message.message.subject == "alice@example.test: busy"
    }));

    let service = ExchangeService::new(storage.clone());
    let mut connect_headers = mapi_headers("Connect");
    connect_headers.insert(
        axum::http::header::AUTHORIZATION,
        HeaderValue::from_static("Bearer delegate-token"),
    );
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &connect_headers, b"")
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
    execute_headers.insert(
        axum::http::header::AUTHORIZATION,
        HeaderValue::from_static("Bearer delegate-token"),
    );
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
        .any(|message| message.subject == "Delegate access for alice@example.test"));
    assert!(stream
        .message_changes
        .iter()
        .any(|message| message.subject == "alice@example.test: busy"));
    assert!(contains_bytes(
        &response_rops,
        &utf16z("IPM.Microsoft.Delegate")
    ));
    assert!(contains_bytes(
        &response_rops,
        &utf16z("IPM.Microsoft.ScheduleData.FreeBusy")
    ));
    assert!(contains_bytes(
        &response_rops,
        &utf16z("meetingObjects=true; sendOnBehalf=true")
    ));

    fixture.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn mapi_over_http_permissions_table_maps_delegate_folder_access() {
    let inbox_id = "55555555-5555-5555-5555-555555555555";
    let delegate_id = Uuid::parse_str("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb").unwrap();
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            inbox_id, "inbox", "Inbox",
        )])),
        mapi_folder_permissions: Arc::new(Mutex::new(vec![
            crate::mapi::permissions::owner_permission(
                Uuid::parse_str(inbox_id).unwrap(),
                &AccountPrincipal {
                    tenant_id: FakeStore::account().tenant_id,
                    account_id: FakeStore::account().account_id,
                    email: FakeStore::account().email,
                    display_name: FakeStore::account().display_name,
                    quota_mb: None,
                    quota_used_octets: None,
                },
            ),
            MapiFolderPermission {
                mailbox_id: Uuid::parse_str(inbox_id).unwrap(),
                member_account_id: Some(delegate_id),
                member_name: "Bob Delegate".to_string(),
                rights: crate::mapi::permissions::rights_from_grant(true, true, false, false),
            },
        ])),
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

    let mut rops = vec![0x02, 0x00, 0x00, 0x01];
    append_mapi_wire_id(&mut rops, test_mapi_folder_id(5));
    rops.push(0);
    rops.extend_from_slice(&[0x3E, 0x00, 0x01, 0x02, 0x00]);
    rops.extend_from_slice(&[0x12, 0x00, 0x02, 0x00]);
    rops.extend_from_slice(&3u16.to_le_bytes());
    rops.extend_from_slice(&0x6671_0014u32.to_le_bytes());
    rops.extend_from_slice(&0x6672_001Fu32.to_le_bytes());
    rops.extend_from_slice(&0x6673_0003u32.to_le_bytes());
    rops.extend_from_slice(&[0x15, 0x00, 0x02, 0x00, 0x01]);
    rops.extend_from_slice(&8u16.to_le_bytes());

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
    assert!(contains_bytes(&response_rops, &[0x3E, 0x02, 0, 0, 0, 0]));
    assert!(contains_bytes(&response_rops, &utf16z("Bob Delegate")));
    assert!(contains_bytes(
        &response_rops,
        &crate::mapi::permissions::rights_from_grant(true, true, false, false).to_le_bytes()
    ));
}

#[tokio::test]
async fn mapi_over_http_ipm_subtree_permissions_table_is_empty_not_not_found() {
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

    let mut rops = vec![0x02, 0x00, 0x00, 0x01];
    append_mapi_wire_id(&mut rops, crate::mapi::identity::IPM_SUBTREE_FOLDER_ID);
    rops.push(0);
    rops.extend_from_slice(&[0x3E, 0x00, 0x01, 0x02, 0x00]);
    rops.extend_from_slice(&[0x12, 0x00, 0x02, 0x00]);
    rops.extend_from_slice(&3u16.to_le_bytes());
    rops.extend_from_slice(&0x6671_0014u32.to_le_bytes());
    rops.extend_from_slice(&0x6672_001Fu32.to_le_bytes());
    rops.extend_from_slice(&0x6673_0003u32.to_le_bytes());
    rops.extend_from_slice(&[0x15, 0x00, 0x02, 0x00, 0x01]);
    rops.extend_from_slice(&8u16.to_le_bytes());

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
    assert!(contains_bytes(&response_rops, &[0x3E, 0x02, 0, 0, 0, 0]));
    assert!(contains_bytes(&response_rops, &[0x12, 0x02, 0, 0, 0, 0, 0]));
    assert!(contains_bytes(&response_rops, &[0x15, 0x02, 0, 0, 0, 0]));
    assert!(!contains_bytes(
        &response_rops,
        &[0x3E, 0x02, 0x0F, 0x01, 0x04, 0x80]
    ));
}

#[tokio::test]
async fn mapi_over_http_modify_permissions_maps_acl_rows_to_canonical_grants() {
    let inbox_id = Uuid::parse_str("55555555-5555-5555-5555-555555555555").unwrap();
    let delegate = AuthenticatedAccount {
        tenant_id: FakeStore::account().tenant_id,
        account_id: Uuid::parse_str("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb").unwrap(),
        email: "bob@example.test".to_string(),
        display_name: "Bob Delegate".to_string(),
        expires_at: "2099-01-01T00:00:00Z".to_string(),
    };
    let delegate_member_id = crate::mapi::identity::mapi_store_id(80);
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            &inbox_id.to_string(),
            "inbox",
            "Inbox",
        )])),
        directory_accounts: Arc::new(Mutex::new(vec![delegate.clone()])),
        mapi_identities: Arc::new(Mutex::new(HashMap::from([(
            delegate.account_id,
            delegate_member_id,
        )]))),
        ..Default::default()
    };
    let observed_permissions = store.mapi_folder_permissions.clone();
    let observed_audits = store.mapi_folder_permission_audits.clone();
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

    let mut rops = vec![0x02, 0x00, 0x00, 0x01];
    append_mapi_wire_id(&mut rops, test_mapi_folder_id(5));
    rops.push(0);
    rops.extend_from_slice(&[0x40, 0x00, 0x01, 0x00]);
    rops.extend_from_slice(&1u16.to_le_bytes());
    rops.push(0x01);
    rops.extend_from_slice(&2u16.to_le_bytes());
    rops.extend_from_slice(&0x6671_0014u32.to_le_bytes());
    rops.extend_from_slice(&(delegate_member_id as i64).to_le_bytes());
    rops.extend_from_slice(&0x6673_0003u32.to_le_bytes());
    rops.extend_from_slice(
        &(crate::mapi::permissions::rights_from_grant(true, true, true, false) as i32)
            .to_le_bytes(),
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
    assert!(contains_bytes(&response_rops, &[0x40, 0x01, 0, 0, 0, 0]));
    let permissions = observed_permissions.lock().unwrap();
    let delegate_permission = permissions
        .iter()
        .find(|permission| permission.member_account_id == Some(delegate.account_id))
        .expect("delegate permission was not written");
    assert_eq!(delegate_permission.mailbox_id, inbox_id);
    assert_eq!(
        delegate_permission.rights,
        crate::mapi::permissions::rights_from_grant(true, true, true, false)
    );
    let audits = observed_audits.lock().unwrap();
    assert_eq!(audits[0].action, "mapi-modify-permissions");
}

#[tokio::test]
async fn mapi_over_http_calendar_modify_permissions_maps_acl_rows_to_calendar_grants() {
    let delegate = AuthenticatedAccount {
        tenant_id: FakeStore::account().tenant_id,
        account_id: Uuid::parse_str("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb").unwrap(),
        email: "bob@example.test".to_string(),
        display_name: "Bob Delegate".to_string(),
        expires_at: "2099-01-01T00:00:00Z".to_string(),
    };
    let delegate_member_id = crate::mapi::identity::mapi_store_id(81);
    let store = FakeStore {
        session: Some(FakeStore::account()),
        directory_accounts: Arc::new(Mutex::new(vec![delegate.clone()])),
        mapi_identities: Arc::new(Mutex::new(HashMap::from([(
            delegate.account_id,
            delegate_member_id,
        )]))),
        ..Default::default()
    };
    let observed_permissions = store.mapi_calendar_permissions.clone();
    let observed_audits = store.mapi_folder_permission_audits.clone();
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
    rops.extend_from_slice(&[0x40, 0x00, 0x01, 0x00]);
    rops.extend_from_slice(&1u16.to_le_bytes());
    rops.push(0x01);
    rops.extend_from_slice(&2u16.to_le_bytes());
    rops.extend_from_slice(&0x6671_0014u32.to_le_bytes());
    rops.extend_from_slice(&(delegate_member_id as i64).to_le_bytes());
    rops.extend_from_slice(&0x6673_0003u32.to_le_bytes());
    rops.extend_from_slice(
        &(crate::mapi::permissions::rights_from_grant(true, true, true, false) as i32)
            .to_le_bytes(),
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
    assert!(contains_bytes(&response_rops, &[0x40, 0x01, 0, 0, 0, 0]));
    let permissions = observed_permissions.lock().unwrap();
    let delegate_permission = permissions
        .iter()
        .find(|permission| permission.member_account_id == Some(delegate.account_id))
        .expect("calendar delegate permission was not written");
    assert_eq!(
        delegate_permission.rights,
        crate::mapi::permissions::rights_from_grant(true, true, true, false)
    );
    let audits = observed_audits.lock().unwrap();
    assert_eq!(audits[0].action, "mapi-modify-calendar-permissions");
}

#[tokio::test]
async fn mapi_over_http_custom_calendar_modify_permissions_maps_acl_rows_to_calendar_grants() {
    let account = FakeStore::account();
    let calendar_collection_id = Uuid::parse_str("cccccccc-cccc-4ccc-8ccc-cccccccccccc").unwrap();
    let delegate = AuthenticatedAccount {
        tenant_id: account.tenant_id,
        account_id: Uuid::parse_str("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb").unwrap(),
        email: "bob@example.test".to_string(),
        display_name: "Bob Delegate".to_string(),
        expires_at: "2099-01-01T00:00:00Z".to_string(),
    };
    let delegate_member_id = crate::mapi::identity::mapi_store_id(82);
    let store = FakeStore {
        session: Some(account.clone()),
        directory_accounts: Arc::new(Mutex::new(vec![delegate.clone()])),
        mapi_identities: Arc::new(Mutex::new(HashMap::from([(
            delegate.account_id,
            delegate_member_id,
        )]))),
        calendar_collections: Arc::new(Mutex::new(vec![FakeStore::collection(
            &calendar_collection_id.to_string(),
            "calendar",
            "Team Calendar",
        )])),
        ..Default::default()
    };
    let snapshot = store
        .load_mapi_mail_store(account.account_id, 100)
        .await
        .unwrap();
    let custom_folder_id = snapshot
        .collaboration_folders()
        .iter()
        .find(|folder| folder.collection.id == calendar_collection_id.to_string())
        .expect("custom calendar folder")
        .id;
    let observed_permissions = store.mapi_calendar_permissions.clone();
    let observed_audits = store.mapi_folder_permission_audits.clone();
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
    append_rop_open_folder(&mut rops, 0, 1, custom_folder_id);
    rops.extend_from_slice(&[0x40, 0x00, 0x01, 0x00]); // RopModifyPermissions.
    rops.extend_from_slice(&1u16.to_le_bytes());
    rops.push(0x01);
    rops.extend_from_slice(&2u16.to_le_bytes());
    rops.extend_from_slice(&0x6671_0014u32.to_le_bytes());
    rops.extend_from_slice(&(delegate_member_id as i64).to_le_bytes());
    rops.extend_from_slice(&0x6673_0003u32.to_le_bytes());
    rops.extend_from_slice(
        &(crate::mapi::permissions::rights_from_grant(true, true, true, true) as i32).to_le_bytes(),
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
    assert!(contains_bytes(&response_rops, &[0x40, 0x01, 0, 0, 0, 0]));
    let permissions = observed_permissions.lock().unwrap();
    let delegate_permission = permissions
        .iter()
        .find(|permission| permission.member_account_id == Some(delegate.account_id))
        .expect("custom calendar delegate permission was not written");
    assert_eq!(delegate_permission.mailbox_id, calendar_collection_id);
    assert_eq!(
        delegate_permission.rights,
        crate::mapi::permissions::rights_from_grant(true, true, true, true)
    );
    let audits = observed_audits.lock().unwrap();
    assert_eq!(audits[0].action, "mapi-modify-calendar-permissions");
    assert!(audits[0]
        .subject
        .contains(&calendar_collection_id.to_string()));
}

#[tokio::test]
async fn mapi_over_http_shared_calendar_with_share_right_modify_permissions_maps_acl_rows_to_calendar_grants(
) {
    let account = FakeStore::account();
    let owner_account_id = Uuid::parse_str("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa").unwrap();
    let calendar_collection_id = Uuid::parse_str("cccccccc-cccc-4ccc-8ccc-cccccccccccd").unwrap();
    let delegate = AuthenticatedAccount {
        tenant_id: account.tenant_id,
        account_id: Uuid::parse_str("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb").unwrap(),
        email: "bob@example.test".to_string(),
        display_name: "Bob Delegate".to_string(),
        expires_at: "2099-01-01T00:00:00Z".to_string(),
    };
    let delegate_member_id = crate::mapi::identity::mapi_store_id(83);
    let mut shared_calendar = FakeStore::collection(
        &calendar_collection_id.to_string(),
        "calendar",
        "Shared Team Calendar",
    );
    shared_calendar.owner_account_id = owner_account_id;
    shared_calendar.owner_email = "owner@example.test".to_string();
    shared_calendar.owner_display_name = "Owner".to_string();
    shared_calendar.is_owned = false;
    shared_calendar.rights.may_read = true;
    shared_calendar.rights.may_write = true;
    shared_calendar.rights.may_delete = true;
    shared_calendar.rights.may_share = true;
    let store = FakeStore {
        session: Some(account.clone()),
        directory_accounts: Arc::new(Mutex::new(vec![delegate.clone()])),
        mapi_identities: Arc::new(Mutex::new(HashMap::from([(
            delegate.account_id,
            delegate_member_id,
        )]))),
        calendar_collections: Arc::new(Mutex::new(vec![shared_calendar])),
        ..Default::default()
    };
    let snapshot = store
        .load_mapi_mail_store(account.account_id, 100)
        .await
        .unwrap();
    let shared_folder_id = snapshot
        .collaboration_folders()
        .iter()
        .find(|folder| folder.collection.id == calendar_collection_id.to_string())
        .expect("shared calendar folder")
        .id;
    let observed_permissions = store.mapi_calendar_permissions.clone();
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
    append_rop_open_folder(&mut rops, 0, 1, shared_folder_id);
    rops.extend_from_slice(&[0x40, 0x00, 0x01, 0x00]); // RopModifyPermissions.
    rops.extend_from_slice(&1u16.to_le_bytes());
    rops.push(0x01);
    rops.extend_from_slice(&2u16.to_le_bytes());
    rops.extend_from_slice(&0x6671_0014u32.to_le_bytes());
    rops.extend_from_slice(&(delegate_member_id as i64).to_le_bytes());
    rops.extend_from_slice(&0x6673_0003u32.to_le_bytes());
    rops.extend_from_slice(
        &(crate::mapi::permissions::rights_from_grant(true, true, false, false) as i32)
            .to_le_bytes(),
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
    assert!(contains_bytes(&response_rops, &[0x40, 0x01, 0, 0, 0, 0]));
    let permissions = observed_permissions.lock().unwrap();
    let delegate_permission = permissions
        .iter()
        .find(|permission| permission.member_account_id == Some(delegate.account_id))
        .expect("shared calendar delegate permission was not written");
    assert_eq!(delegate_permission.mailbox_id, calendar_collection_id);
    assert_eq!(
        delegate_permission.rights,
        crate::mapi::permissions::rights_from_grant(true, true, false, false)
    );
}

#[tokio::test]
async fn mapi_over_http_custom_calendar_modify_permissions_remove_deletes_calendar_grant() {
    let account = FakeStore::account();
    let calendar_collection_id = Uuid::parse_str("cccccccc-cccc-4ccc-8ccc-ccccccccccce").unwrap();
    let delegate = AuthenticatedAccount {
        tenant_id: account.tenant_id,
        account_id: Uuid::parse_str("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb").unwrap(),
        email: "bob@example.test".to_string(),
        display_name: "Bob Delegate".to_string(),
        expires_at: "2099-01-01T00:00:00Z".to_string(),
    };
    let delegate_member_id = crate::mapi::identity::mapi_store_id(84);
    let store = FakeStore {
        session: Some(account.clone()),
        directory_accounts: Arc::new(Mutex::new(vec![delegate.clone()])),
        mapi_identities: Arc::new(Mutex::new(HashMap::from([(
            delegate.account_id,
            delegate_member_id,
        )]))),
        calendar_collections: Arc::new(Mutex::new(vec![FakeStore::collection(
            &calendar_collection_id.to_string(),
            "calendar",
            "Team Calendar",
        )])),
        mapi_calendar_permissions: Arc::new(Mutex::new(vec![MapiFolderPermission {
            mailbox_id: calendar_collection_id,
            member_account_id: Some(delegate.account_id),
            member_name: delegate.display_name.clone(),
            rights: crate::mapi::permissions::rights_from_grant(true, true, false, false),
        }])),
        ..Default::default()
    };
    let snapshot = store
        .load_mapi_mail_store(account.account_id, 100)
        .await
        .unwrap();
    let custom_folder_id = snapshot
        .collaboration_folders()
        .iter()
        .find(|folder| folder.collection.id == calendar_collection_id.to_string())
        .expect("custom calendar folder")
        .id;
    let observed_permissions = store.mapi_calendar_permissions.clone();
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
    append_rop_open_folder(&mut rops, 0, 1, custom_folder_id);
    rops.extend_from_slice(&[0x40, 0x00, 0x01, 0x00]); // RopModifyPermissions.
    rops.extend_from_slice(&1u16.to_le_bytes());
    rops.push(0x04);
    rops.extend_from_slice(&1u16.to_le_bytes());
    rops.extend_from_slice(&0x6671_0014u32.to_le_bytes());
    rops.extend_from_slice(&(delegate_member_id as i64).to_le_bytes());

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
    assert!(contains_bytes(&response_rops, &[0x40, 0x01, 0, 0, 0, 0]));
    let permissions = observed_permissions.lock().unwrap();
    assert!(!permissions.iter().any(|permission| {
        permission.mailbox_id == calendar_collection_id
            && permission.member_account_id == Some(delegate.account_id)
    }));
}

#[tokio::test]
async fn mapi_over_http_shared_calendar_without_share_right_rejects_modify_permissions() {
    let account = FakeStore::account();
    let owner_account_id = Uuid::parse_str("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa").unwrap();
    let calendar_collection_id = Uuid::parse_str("cccccccc-cccc-4ccc-8ccc-cccccccccccf").unwrap();
    let delegate = AuthenticatedAccount {
        tenant_id: account.tenant_id,
        account_id: Uuid::parse_str("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb").unwrap(),
        email: "bob@example.test".to_string(),
        display_name: "Bob Delegate".to_string(),
        expires_at: "2099-01-01T00:00:00Z".to_string(),
    };
    let delegate_member_id = crate::mapi::identity::mapi_store_id(85);
    let mut shared_calendar = FakeStore::collection(
        &calendar_collection_id.to_string(),
        "calendar",
        "Readonly Shared Team Calendar",
    );
    shared_calendar.owner_account_id = owner_account_id;
    shared_calendar.owner_email = "owner@example.test".to_string();
    shared_calendar.owner_display_name = "Owner".to_string();
    shared_calendar.is_owned = false;
    shared_calendar.rights.may_read = true;
    shared_calendar.rights.may_write = true;
    shared_calendar.rights.may_delete = false;
    shared_calendar.rights.may_share = false;
    let store = FakeStore {
        session: Some(account.clone()),
        directory_accounts: Arc::new(Mutex::new(vec![delegate.clone()])),
        mapi_identities: Arc::new(Mutex::new(HashMap::from([(
            delegate.account_id,
            delegate_member_id,
        )]))),
        calendar_collections: Arc::new(Mutex::new(vec![shared_calendar])),
        ..Default::default()
    };
    let snapshot = store
        .load_mapi_mail_store(account.account_id, 100)
        .await
        .unwrap();
    let shared_folder_id = snapshot
        .collaboration_folders()
        .iter()
        .find(|folder| folder.collection.id == calendar_collection_id.to_string())
        .expect("shared calendar folder")
        .id;
    let observed_permissions = store.mapi_calendar_permissions.clone();
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
    append_rop_open_folder(&mut rops, 0, 1, shared_folder_id);
    rops.extend_from_slice(&[0x40, 0x00, 0x01, 0x00]); // RopModifyPermissions.
    rops.extend_from_slice(&1u16.to_le_bytes());
    rops.push(0x01);
    rops.extend_from_slice(&2u16.to_le_bytes());
    rops.extend_from_slice(&0x6671_0014u32.to_le_bytes());
    rops.extend_from_slice(&(delegate_member_id as i64).to_le_bytes());
    rops.extend_from_slice(&0x6673_0003u32.to_le_bytes());
    rops.extend_from_slice(
        &(crate::mapi::permissions::rights_from_grant(true, true, false, false) as i32)
            .to_le_bytes(),
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
    assert!(!contains_bytes(&response_rops, &[0x40, 0x01, 0, 0, 0, 0]));
    assert!(contains_bytes(
        &response_rops,
        &0x8007_0005u32.to_le_bytes()
    ));
    let permissions = observed_permissions.lock().unwrap();
    assert!(!permissions.iter().any(|permission| {
        permission.mailbox_id == calendar_collection_id
            && permission.member_account_id == Some(delegate.account_id)
    }));
}

#[tokio::test]
async fn mapi_over_http_denies_mutation_without_folder_write_permission() {
    let inbox_id = "55555555-5555-5555-5555-555555555555";
    let account = FakeStore::account();
    let store = FakeStore {
        session: Some(account.clone()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            inbox_id, "inbox", "Inbox",
        )])),
        mapi_folder_permissions: Arc::new(Mutex::new(vec![MapiFolderPermission {
            mailbox_id: Uuid::parse_str(inbox_id).unwrap(),
            member_account_id: Some(account.account_id),
            member_name: account.display_name,
            rights: crate::mapi::permissions::rights_from_grant(true, false, false, false),
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

    let mut rops = vec![0x02, 0x00, 0x00, 0x01];
    append_mapi_wire_id(&mut rops, test_mapi_folder_id(5));
    rops.push(0);
    append_rop_create_message(&mut rops, 1, 2, test_mapi_folder_id(5));

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
        &[0x06, 0x02, 0x05, 0x00, 0x07, 0x80]
    ));
}

#[tokio::test]
async fn mapi_over_http_denies_contents_table_without_folder_read_permission() {
    let inbox_id = "55555555-5555-5555-5555-555555555555";
    let account = FakeStore::account();
    let store = FakeStore {
        session: Some(account.clone()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            inbox_id, "inbox", "Inbox",
        )])),
        mapi_folder_permissions: Arc::new(Mutex::new(vec![MapiFolderPermission {
            mailbox_id: Uuid::parse_str(inbox_id).unwrap(),
            member_account_id: Some(account.account_id),
            member_name: account.display_name,
            rights: crate::mapi::permissions::rights_from_grant(false, false, false, false),
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

    let mut rops = vec![0x02, 0x00, 0x00, 0x01];
    append_mapi_wire_id(&mut rops, test_mapi_folder_id(5));
    rops.push(0);
    rops.extend_from_slice(&[0x05, 0x00, 0x01, 0x02, 0x00]);

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
        &[0x05, 0x02, 0x05, 0x00, 0x07, 0x80]
    ));
}

#[tokio::test]
async fn mapi_over_http_empty_folder_rejects_unsupported_and_permission_denied_targets() {
    let trash_id = Uuid::parse_str("77777777-7777-7777-7777-777777777777").unwrap();
    let account = FakeStore::account();
    let store = FakeStore {
        session: Some(account.clone()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            &trash_id.to_string(),
            "trash",
            "Deleted Items",
        )])),
        mapi_folder_permissions: Arc::new(Mutex::new(vec![MapiFolderPermission {
            mailbox_id: trash_id,
            member_account_id: Some(account.account_id),
            member_name: account.display_name,
            rights: rights_from_grant(true, true, false, false),
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

    let mut rops = vec![0x02, 0x00, 0x00, 0x01];
    append_mapi_wire_id(&mut rops, crate::mapi::identity::TRASH_FOLDER_ID);
    rops.push(0);
    rops.extend_from_slice(&[0x58, 0x00, 0x01, 0x00, 0x00]);
    rops.extend_from_slice(&[0x02, 0x00, 0x00, 0x02]);
    append_mapi_wire_id(&mut rops, crate::mapi::identity::CALENDAR_FOLDER_ID);
    rops.push(0);
    rops.extend_from_slice(&[0x92, 0x00, 0x02, 0x00, 0x00]);
    for (slot, folder_id) in [
        (3, crate::mapi::identity::CONTACTS_FOLDER_ID),
        (4, crate::mapi::identity::TASKS_FOLDER_ID),
        (5, crate::mapi::identity::NOTES_FOLDER_ID),
        (6, crate::mapi::identity::JOURNAL_FOLDER_ID),
    ] {
        rops.extend_from_slice(&[0x02, 0x00, 0x00, slot]);
        append_mapi_wire_id(&mut rops, folder_id);
        rops.push(0);
        rops.extend_from_slice(&[0x58, 0x00, slot, 0x00, 0x00]);
    }

    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(
                &rops,
                &[
                    1,
                    u32::MAX,
                    u32::MAX,
                    u32::MAX,
                    u32::MAX,
                    u32::MAX,
                    u32::MAX,
                    u32::MAX,
                    u32::MAX,
                    u32::MAX,
                    u32::MAX,
                ],
            )),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(
        &response_rops,
        &[0x58, 0x01, 0x05, 0x00, 0x07, 0x80]
    ));
    assert!(contains_bytes(
        &response_rops,
        &[0x92, 0x02, 0x0F, 0x01, 0x04, 0x80]
    ));
    for handle in [3, 4, 5, 6] {
        assert!(contains_bytes(
            &response_rops,
            &[0x58, handle, 0x0F, 0x01, 0x04, 0x80]
        ));
    }
}
