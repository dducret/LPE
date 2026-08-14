use super::*;

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
    let (mut execute_headers, logon_handle) = mapi_connect_with_private_logon(&service).await;

    // Probe M sends this exact [MS-OXCPERM] section 2.2.2.1 empty
    // ReplaceRows request against the fresh Freebusy Data permission table.
    let mut empty_freebusy_rops = Vec::new();
    append_rop_open_folder(
        &mut empty_freebusy_rops,
        0,
        1,
        crate::mapi::identity::FREEBUSY_DATA_FOLDER_ID,
    );
    empty_freebusy_rops.extend_from_slice(&[0x40, 0x00, 0x01, 0x01]);
    empty_freebusy_rops.extend_from_slice(&0u16.to_le_bytes());
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&empty_freebusy_rops, &[logon_handle, u32::MAX])),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(&response_rops, &[0x40, 0x01, 0, 0, 0, 0]));

    renew_mapi_request_id(&mut execute_headers);
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
            &execute_body(&rop_buffer(&rops, &[logon_handle, u32::MAX])),
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

    // The Freebusy Data ACL surface is intentionally fail-closed while its
    // distinct role semantics are not representable. Once a canonical
    // delegate exists, an empty ReplaceRows request is no longer the fresh
    // empty-table no-op and must not falsely report that it removed the row.
    renew_mapi_request_id(&mut execute_headers);
    let mut nonempty_freebusy_rops = Vec::new();
    append_rop_open_folder(
        &mut nonempty_freebusy_rops,
        0,
        1,
        crate::mapi::identity::FREEBUSY_DATA_FOLDER_ID,
    );
    nonempty_freebusy_rops.extend_from_slice(&[0x40, 0x00, 0x01, 0x01]);
    nonempty_freebusy_rops.extend_from_slice(&0u16.to_le_bytes());
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(
                &nonempty_freebusy_rops,
                &[logon_handle, u32::MAX],
            )),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(
        &response_rops,
        &[0x40, 0x01, 0x0F, 0x01, 0x04, 0x80]
    ));

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
        INSERT INTO account_credentials (tenant_id, account_email, password_hash)
        VALUES ($1, 'delegate@example.test', 'test-hash')
        "#,
    )
    .bind(tenant_id)
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
    storage
        .ensure_jmap_system_mailboxes(fixture.account_id)
        .await?;
    storage
        .upsert_mailbox_delegation_grant_with_preferences(
            lpe_storage::MailboxDelegationGrantInput {
                owner_account_id: fixture.account_id,
                grantee_email: "delegate@example.test".to_string(),
                may_write: true,
            },
            lpe_storage::DelegatePreferencesPatch {
                meeting_request_delivery: Some("delegate_only".to_string()),
                receives_meeting_request_copy: Some(true),
                may_view_private_items: Some(true),
            },
            lpe_storage::AuditEntryInput {
                actor: "alice@example.test".to_string(),
                action: "test-delegate-preferences".to_string(),
                subject: "delegate@example.test".to_string(),
            },
        )
        .await?;

    let snapshot = storage
        .load_mapi_mail_store(delegate_account_id, 500)
        .await?;
    assert_eq!(snapshot.delegate_freebusy_messages().len(), 3);
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
    let local_freebusy = snapshot
        .delegate_freebusy_messages()
        .iter()
        .find(|message| crate::mapi_store::is_outlook_local_freebusy_message(message))
        .expect("delegate snapshot contains its durable LocalFreebusy message");
    assert_eq!(local_freebusy.message.subject, "LocalFreebusy");
    assert!(local_freebusy.delegates.is_empty());
    let local_freebusy_id = local_freebusy.id;
    let local_freebusy_identity = local_freebusy
        .durable_identity
        .clone()
        .expect("LocalFreebusy has a durable MAPI identity");
    let identity_codec = snapshot.identity_codec().clone();
    let projection_state_before = sqlx::query_as::<_, (i64, i64)>(
        r#"
        SELECT revision, applied_revision
        FROM delegation_projection_state
        WHERE tenant_id = $1 AND account_id = $2
        "#,
    )
    .bind(tenant_id)
    .bind(delegate_account_id)
    .fetch_one(storage.pool())
    .await?;

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
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert(
        axum::http::header::AUTHORIZATION,
        HeaderValue::from_static("Bearer delegate-token"),
    );
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let logon = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(
                &mapi_private_logon_rops("delegate"),
                &[u32::MAX],
            )),
        )
        .await
        .unwrap();
    let logon_body = response_bytes(logon).await;
    let (_, logon_handles) = response_rops_and_handles_from_execute_body(&logon_body);
    let logon_handle = logon_handles[0];
    renew_mapi_request_id(&mut execute_headers);
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
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[logon_handle, u32::MAX, u32::MAX])),
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

    renew_mapi_request_id(&mut execute_headers);
    let mut normal_sync_rops = Vec::new();
    append_rop_open_folder(
        &mut normal_sync_rops,
        0,
        1,
        crate::mapi::identity::FREEBUSY_DATA_FOLDER_ID,
    );
    normal_sync_rops.extend_from_slice(&[
        0x70, 0x00, 0x01, 0x02, // RopSynchronizationConfigure
        0x01, 0x00, 0x00, 0x00, // content sync, normal messages only
        0x00, 0x00, // RestrictionDataSize
        0x05, 0x00, 0x00, 0x00, // SynchronizationExtraFlags: Eid | CN
        0x00, 0x00, // PropertyTagCount
        0x4E, 0x00, 0x02, // RopFastTransferSourceGetBuffer
    ]);
    normal_sync_rops.extend_from_slice(&4096u16.to_le_bytes());
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(
                &normal_sync_rops,
                &[logon_handle, u32::MAX, u32::MAX],
            )),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    assert_eq!(mapi_sync_manifest_counts(&response_rops), Some((0, 1)));
    let normal_stream = strict_content_sync_transfer_from_response(&response_rops).unwrap();
    assert_eq!(normal_stream.message_changes.len(), 1);
    let local_change = &normal_stream.message_changes[0];
    assert!(!local_change.associated);
    assert_eq!(local_change.subject, "LocalFreebusy");
    assert_eq!(local_change.source_key, local_freebusy_identity.source_key);
    assert_eq!(local_change.change_key, local_freebusy_identity.change_key);
    assert_eq!(
        local_change.predecessor_change_list,
        local_freebusy_identity.predecessor_change_list
    );
    assert_eq!(
        local_change.change_number,
        Some(local_freebusy_identity.change_number)
    );
    assert_eq!(
        local_change.last_modification_time,
        Some(local_freebusy_identity.last_modification_time)
    );
    assert_eq!(local_change.mid, Some(local_freebusy_id));
    assert!(local_change.entry_id.is_none());
    assert!(!local_change.body_tags.contains(&0x0FF6_0102));
    assert_eq!(
        local_change
            .body_properties
            .iter()
            .find(|(tag, _)| *tag == PID_TAG_MESSAGE_FLAGS)
            .map(|(_, value)| u32::from_le_bytes(value.as_slice().try_into().unwrap())),
        Some(0),
        "normal LocalFreebusy must not carry MSGFLAG_ASSOCIATED"
    );

    renew_mapi_request_id(&mut execute_headers);
    let mut normal_table_rops = Vec::new();
    append_rop_open_folder(
        &mut normal_table_rops,
        0,
        1,
        crate::mapi::identity::FREEBUSY_DATA_FOLDER_ID,
    );
    normal_table_rops.extend_from_slice(&[
        0x05, 0x00, 0x01, 0x02, 0x00, // normal RopGetContentsTable
        0x12, 0x00, 0x02, 0x00, // RopSetColumns
    ]);
    normal_table_rops.extend_from_slice(&7u16.to_le_bytes());
    normal_table_rops.extend_from_slice(&PID_TAG_FOLDER_ID.to_le_bytes());
    normal_table_rops.extend_from_slice(&PID_TAG_MID.to_le_bytes());
    normal_table_rops.extend_from_slice(&PID_TAG_INST_ID.to_le_bytes());
    normal_table_rops.extend_from_slice(&PID_TAG_INSTANCE_NUM.to_le_bytes());
    normal_table_rops.extend_from_slice(&PID_TAG_SUBJECT_W.to_le_bytes());
    normal_table_rops.extend_from_slice(&PID_TAG_MESSAGE_FLAGS.to_le_bytes());
    normal_table_rops.extend_from_slice(&PID_TAG_ASSOCIATED.to_le_bytes());
    normal_table_rops.extend_from_slice(&[0x15, 0x00, 0x02, 0x00, 0x01]);
    normal_table_rops.extend_from_slice(&50u16.to_le_bytes());
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(
                &normal_table_rops,
                &[logon_handle, u32::MAX, u32::MAX],
            )),
        )
        .await
        .unwrap();
    let response_rops = response_rops_from_execute_response(response).await;
    let contents_offset = 8;
    assert_eq!(response_rops[contents_offset], 0x05);
    assert_eq!(
        u32::from_le_bytes(
            response_rops[contents_offset + 6..contents_offset + 10]
                .try_into()
                .unwrap()
        ),
        1
    );
    let query_offset = contents_offset + 10 + 7;
    assert_eq!(response_rops[query_offset], 0x15);
    assert_eq!(
        u16::from_le_bytes(
            response_rops[query_offset + 7..query_offset + 9]
                .try_into()
                .unwrap()
        ),
        1
    );
    let mut row_offset = query_offset + 9;
    assert_eq!(response_rops[row_offset], 0);
    row_offset += 1;
    assert_eq!(
        identity_codec.object_id_from_wire_id(&response_rops[row_offset..row_offset + 8]),
        Some(crate::mapi::identity::FREEBUSY_DATA_FOLDER_ID)
    );
    row_offset += 8;
    assert_eq!(
        identity_codec.object_id_from_wire_id(&response_rops[row_offset..row_offset + 8]),
        Some(local_freebusy_id)
    );
    row_offset += 8;
    assert_eq!(
        u64::from_le_bytes(
            response_rops[row_offset..row_offset + 8]
                .try_into()
                .unwrap()
        ),
        local_freebusy_id
    );
    row_offset += 8;
    assert_eq!(
        u32::from_le_bytes(
            response_rops[row_offset..row_offset + 4]
                .try_into()
                .unwrap()
        ),
        0
    );
    row_offset += 4;
    assert_eq!(
        read_rop_utf16z(&response_rops, &mut row_offset).unwrap(),
        "LocalFreebusy"
    );
    assert_eq!(
        u32::from_le_bytes(
            response_rops[row_offset..row_offset + 4]
                .try_into()
                .unwrap()
        ),
        0
    );
    row_offset += 4;
    assert_eq!(response_rops[row_offset], 0);

    renew_mapi_request_id(&mut execute_headers);
    let mut associated_table_rops = Vec::new();
    append_rop_open_folder(
        &mut associated_table_rops,
        0,
        1,
        crate::mapi::identity::FREEBUSY_DATA_FOLDER_ID,
    );
    associated_table_rops.extend_from_slice(&[
        0x05, 0x00, 0x01, 0x02, 0x02, // associated RopGetContentsTable
    ]);
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(
                &associated_table_rops,
                &[logon_handle, u32::MAX, u32::MAX],
            )),
        )
        .await
        .unwrap();
    let response_rops = response_rops_from_execute_response(response).await;
    assert_eq!(response_rops[8], 0x05);
    assert_eq!(
        u32::from_le_bytes(response_rops[14..18].try_into().unwrap()),
        2,
        "only the two computed delegate/free-busy rows belong to the FAI table"
    );

    renew_mapi_request_id(&mut execute_headers);
    let mut entry_id_rops = Vec::new();
    append_rop_open_folder(
        &mut entry_id_rops,
        0,
        1,
        crate::mapi::identity::ROOT_FOLDER_ID,
    );
    append_rop_get_properties_specific(&mut entry_id_rops, 1, &[0x36E4_1102]);
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&entry_id_rops, &[logon_handle, u32::MAX])),
        )
        .await
        .unwrap();
    let response_rops = response_rops_from_execute_response(response).await;
    let entry_id_row = mapi_get_properties_specific_standard_row_offset(&response_rops, 1)
        .expect("Root FreeBusyEntryIds GetProps row");
    let mut value_offset = entry_id_row + 1;
    let entry_id_count = u32::from_le_bytes(
        response_rops[value_offset..value_offset + 4]
            .try_into()
            .unwrap(),
    ) as usize;
    value_offset += 4;
    let mut free_busy_entry_ids = Vec::with_capacity(entry_id_count);
    for _ in 0..entry_id_count {
        free_busy_entry_ids.push(
            read_rop_binary_u16(&response_rops, &mut value_offset)
                .unwrap()
                .to_vec(),
        );
    }
    assert_eq!(free_busy_entry_ids.len(), 4);
    assert!(free_busy_entry_ids[0].is_empty());
    assert!(free_busy_entry_ids[2].is_empty());
    let advertised_local_entry_id = &free_busy_entry_ids[1];
    assert_eq!(advertised_local_entry_id.len(), 70);
    let advertised_target = identity_codec
        .object_ids_from_message_entry_id(delegate_account_id, advertised_local_entry_id)
        .expect("FreeBusyEntryIds advertises a provider Message EntryID");
    assert_eq!(
        advertised_target,
        (
            crate::mapi::identity::FREEBUSY_DATA_FOLDER_ID,
            local_freebusy_id
        )
    );

    const EXCHANGE_RAW548_TAGS: [u32; 18] = [
        0x6841_0003,
        0x6842_000B,
        0x6843_000B,
        0x684A_101F,
        0x6845_1102,
        0x686B_1003,
        0x6870_1102,
        0x6871_1003,
        0x6872_001F,
        0x686D_000B,
        0x686E_000B,
        0x686F_000B,
        0x684B_000B,
        0x6844_101F,
        0x3008_0040,
        0x65E2_0102,
        0x0E0B_0102,
        0x001A_001F,
    ];
    renew_mapi_request_id(&mut execute_headers);
    let mut get_rops = Vec::new();
    append_rop_open_message(
        &mut get_rops,
        0,
        1,
        advertised_target.0,
        advertised_target.1,
    );
    append_rop_get_properties_specific(&mut get_rops, 1, &EXCHANGE_RAW548_TAGS);
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&get_rops, &[logon_handle, u32::MAX])),
        )
        .await
        .unwrap();
    let response_rops = response_rops_from_execute_response(response).await;
    let marker = [0x07, 0x01, 0, 0, 0, 0];
    let get_offset = response_rops
        .windows(marker.len())
        .position(|window| window == marker)
        .expect("LocalFreebusy raw548 GetProps response");
    let mut value_offset = get_offset + marker.len();
    assert_eq!(response_rops[value_offset], 1);
    value_offset += 1;
    for property_tag in EXCHANGE_RAW548_TAGS.iter().take(14) {
        assert_eq!(
            response_rops[value_offset], 0x0A,
            "fresh LocalFreebusy {property_tag:#010x} must be absent"
        );
        value_offset += 1;
        assert_eq!(
            u32::from_le_bytes(
                response_rops[value_offset..value_offset + 4]
                    .try_into()
                    .unwrap()
            ),
            0x8004_010F
        );
        value_offset += 4;
    }
    assert_eq!(response_rops[value_offset], 0);
    value_offset += 1;
    assert_eq!(
        u64::from_le_bytes(
            response_rops[value_offset..value_offset + 8]
                .try_into()
                .unwrap()
        ),
        local_freebusy_identity.last_modification_time
    );
    value_offset += 8;
    assert_eq!(response_rops[value_offset], 0);
    value_offset += 1;
    assert_eq!(
        read_rop_binary_u16(&response_rops, &mut value_offset).unwrap(),
        local_freebusy_identity.change_key
    );
    assert_eq!(response_rops[value_offset], 0);
    value_offset += 1;
    let provider_entry_id = read_rop_binary_u16(&response_rops, &mut value_offset).unwrap();
    assert_eq!(provider_entry_id.len(), 46);
    assert_eq!(
        provider_entry_id,
        crate::mapi::identity::outlook_message_list_settings_entry_id(
            delegate_account_id,
            local_freebusy_id,
        )
        .unwrap()
    );
    assert_eq!(response_rops[value_offset], 0);
    value_offset += 1;
    assert_eq!(
        read_rop_utf16z(&response_rops, &mut value_offset).unwrap(),
        "IPM.Microsoft.ScheduleData.FreeBusy"
    );
    // Probe M follows the successful table/open path by creating and writing
    // this provider-private named flag on LocalFreebusy. It is compatibility
    // metadata on the Delegate Information object, not canonical grant state.
    let ps_public_strings = [
        0x29, 0x03, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0xC0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x46,
    ];
    let fixupfbfolder_name = utf16z("urn:schemas-microsoft-com:office:outlook#fixupfbfolder");
    renew_mapi_request_id(&mut execute_headers);
    let mut named_rops = Vec::new();
    named_rops.extend_from_slice(&[
        0x56, 0x00, 0x00, 0x02, // RopGetPropertyIdsFromNames, create missing on Logon.
    ]);
    named_rops.extend_from_slice(&1u16.to_le_bytes());
    named_rops.push(0x01); // MNID_STRING.
    named_rops.extend_from_slice(&ps_public_strings);
    named_rops.push(fixupfbfolder_name.len() as u8);
    named_rops.extend_from_slice(&fixupfbfolder_name);
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&named_rops, &[logon_handle])),
        )
        .await
        .unwrap();
    let response_rops = response_rops_from_execute_response(response).await;
    let named_marker = [0x56, 0x00, 0, 0, 0, 0, 1, 0];
    let named_offset = response_rops
        .windows(named_marker.len())
        .position(|window| window == named_marker)
        .expect("LocalFreebusy fixupfbfolder named-property mapping");
    let fixupfbfolder_id = u16::from_le_bytes(
        response_rops[named_offset + named_marker.len()..named_offset + named_marker.len() + 2]
            .try_into()
            .unwrap(),
    );
    assert!(fixupfbfolder_id >= crate::mapi::properties::DYNAMIC_NAMED_PROPERTY_ID_START);
    let fixupfbfolder_tag = u32::from(fixupfbfolder_id) << 16 | 0x000B;

    // [MS-OXCPRPT] section 3.2.5.4: a Message SetProperties value is
    // handle-local until SaveChangesMessage. Prove the pending value through
    // all three property reads while a separately opened handle still sees
    // the durable absence.
    renew_mapi_request_id(&mut execute_headers);
    let mut staged_fixup_rops = Vec::new();
    append_rop_open_message(
        &mut staged_fixup_rops,
        0,
        1,
        advertised_target.0,
        advertised_target.1,
    );
    let mut true_fixup_value = Vec::new();
    append_mapi_bool_property(&mut true_fixup_value, fixupfbfolder_tag, true);
    append_rop_set_properties(&mut staged_fixup_rops, 1, 1, &true_fixup_value);
    append_rop_get_properties_specific(&mut staged_fixup_rops, 1, &[fixupfbfolder_tag]);
    staged_fixup_rops.extend_from_slice(&[0x08, 0x00, 0x01, 0x00, 0x10, 0x01, 0x00]);
    staged_fixup_rops.extend_from_slice(&[0x09, 0x00, 0x01]);
    append_rop_open_message(
        &mut staged_fixup_rops,
        0,
        2,
        advertised_target.0,
        advertised_target.1,
    );
    append_rop_get_properties_specific(&mut staged_fixup_rops, 2, &[fixupfbfolder_tag]);
    staged_fixup_rops.extend_from_slice(&[0x01, 0x00, 0x01, 0x01, 0x00, 0x02]);
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(
                &staged_fixup_rops,
                &[logon_handle, u32::MAX, u32::MAX],
            )),
        )
        .await
        .unwrap();
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(&response_rops, &[0x0A, 0x01, 0, 0, 0, 0]));
    let row_offset = mapi_get_properties_specific_standard_row_offset(&response_rops, 1)
        .expect("pending LocalFreebusy fixupfbfolder GetProps");
    assert_eq!(response_rops[row_offset + 1], 1);
    let get_all_start = response_rops
        .windows(6)
        .position(|window| window == [0x08, 0x01, 0, 0, 0, 0])
        .expect("pending LocalFreebusy GetPropertiesAll");
    let get_list_start = response_rops[get_all_start + 6..]
        .windows(6)
        .position(|window| window == [0x09, 0x01, 0, 0, 0, 0])
        .map(|offset| get_all_start + 6 + offset)
        .expect("pending LocalFreebusy GetPropertiesList");
    let second_open_start = response_rops[get_list_start + 6..]
        .windows(6)
        .position(|window| window == [0x03, 0x02, 0, 0, 0, 0])
        .map(|offset| get_list_start + 6 + offset)
        .expect("second LocalFreebusy OpenMessage");
    let mut expected_fixup_property = fixupfbfolder_tag.to_le_bytes().to_vec();
    expected_fixup_property.push(1);
    assert!(contains_bytes(
        &response_rops[get_all_start..get_list_start],
        &expected_fixup_property
    ));
    assert!(contains_bytes(
        &response_rops[get_list_start..second_open_start],
        &fixupfbfolder_tag.to_le_bytes()
    ));
    let second_marker = [0x07, 0x02, 0, 0, 0, 0];
    let second_row = response_rops
        .windows(second_marker.len())
        .position(|window| window == second_marker)
        .map(|offset| offset + second_marker.len())
        .expect("second LocalFreebusy fixupfbfolder GetProps");
    assert_eq!(response_rops[second_row], 1);
    assert_eq!(response_rops[second_row + 1], 0x0A);
    assert_eq!(
        u32::from_le_bytes(
            response_rops[second_row + 2..second_row + 6]
                .try_into()
                .unwrap()
        ),
        0x8004_010F
    );
    let staged_row_count = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*)
        FROM mapi_custom_property_values
        WHERE tenant_id = $1
          AND account_id = $2
          AND object_kind = 'delegate_freebusy_message'
          AND canonical_id = $3
          AND property_tag = $4
        "#,
    )
    .bind(tenant_id)
    .bind(delegate_account_id)
    .bind(crate::mapi_store::OUTLOOK_LOCAL_FREEBUSY_CANONICAL_ID)
    .bind(i64::from(fixupfbfolder_tag))
    .fetch_one(storage.pool())
    .await?;
    assert_eq!(
        staged_row_count, 0,
        "SetProperties must not persist before Save"
    );

    // A distinct live session subscribes before the save. Its later Execute
    // must receive the same durable change without relying on handle-local
    // state from the saving session.
    let mut observer_connect_headers = mapi_headers("Connect");
    observer_connect_headers.insert(
        axum::http::header::AUTHORIZATION,
        HeaderValue::from_static("Bearer delegate-token"),
    );
    let observer_connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &observer_connect_headers, b"")
        .await
        .unwrap();
    let observer_cookie = mapi_cookie_header(&observer_connect);
    let mut observer_execute_headers = mapi_headers("Execute");
    observer_execute_headers.insert(
        axum::http::header::AUTHORIZATION,
        HeaderValue::from_static("Bearer delegate-token"),
    );
    observer_execute_headers.insert("cookie", HeaderValue::from_str(&observer_cookie).unwrap());
    let observer_logon = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &observer_execute_headers,
            &execute_body(&rop_buffer(
                &mapi_private_logon_rops("delegate"),
                &[u32::MAX],
            )),
        )
        .await
        .unwrap();
    let observer_logon_body = response_bytes(observer_logon).await;
    let (_, observer_logon_handles) =
        response_rops_and_handles_from_execute_body(&observer_logon_body);
    let observer_logon_handle = observer_logon_handles[0];
    renew_mapi_request_id(&mut observer_execute_headers);
    let mut observer_registration = vec![0x29, 0x00, 0x00, 0x01];
    observer_registration.extend_from_slice(&0x0010u16.to_le_bytes());
    observer_registration.push(0);
    observer_registration.extend_from_slice(
        &identity_codec
            .wire_id_bytes_from_object_id(advertised_target.0)
            .unwrap(),
    );
    observer_registration.extend_from_slice(
        &identity_codec
            .wire_id_bytes_from_object_id(advertised_target.1)
            .unwrap(),
    );
    let observer_registered = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &observer_execute_headers,
            &execute_body(&rop_buffer(
                &observer_registration,
                &[observer_logon_handle, u32::MAX],
            )),
        )
        .await
        .unwrap();
    let observer_registered_rops = response_rops_from_execute_response(observer_registered).await;
    assert!(contains_bytes(
        &observer_registered_rops,
        &[0x29, 0x01, 0, 0, 0, 0]
    ));

    // Save the nondefault value and require the same Execute buffer to see
    // the committed custom value, the newly rotated durable identity, and the
    // message-scoped ObjectModified notification registered by Probe M.
    renew_mapi_request_id(&mut execute_headers);
    let mut save_fixup_rops = Vec::new();
    append_rop_open_message(
        &mut save_fixup_rops,
        0,
        1,
        advertised_target.0,
        advertised_target.1,
    );
    save_fixup_rops.extend_from_slice(&[0x29, 0x00, 0x00, 0x02]);
    save_fixup_rops.extend_from_slice(&0x0010u16.to_le_bytes()); // ObjectModified.
    save_fixup_rops.push(0); // Scope is the following FID/MID pair.
    save_fixup_rops.extend_from_slice(
        &identity_codec
            .wire_id_bytes_from_object_id(advertised_target.0)
            .unwrap(),
    );
    save_fixup_rops.extend_from_slice(
        &identity_codec
            .wire_id_bytes_from_object_id(advertised_target.1)
            .unwrap(),
    );
    append_rop_set_properties(&mut save_fixup_rops, 1, 1, &true_fixup_value);
    append_rop_save_changes_message_with_flags(&mut save_fixup_rops, 1, 1, 0x0A);
    append_rop_get_properties_specific(
        &mut save_fixup_rops,
        1,
        &[
            fixupfbfolder_tag,
            PID_TAG_SOURCE_KEY,
            PID_TAG_CHANGE_KEY,
            PID_TAG_PREDECESSOR_CHANGE_LIST,
            PID_TAG_CHANGE_NUMBER,
            PID_TAG_LAST_MODIFICATION_TIME,
        ],
    );
    // A second Save on the now-clean retained handle is a no-op and must not
    // produce another ObjectModified notification or identity rotation.
    append_rop_save_changes_message_with_flags(&mut save_fixup_rops, 1, 1, 0x0A);
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(
                &save_fixup_rops,
                &[logon_handle, u32::MAX, u32::MAX],
            )),
        )
        .await
        .unwrap();
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(&response_rops, &[0x29, 0x02, 0, 0, 0, 0]));
    assert!(contains_bytes(&response_rops, &[0x0A, 0x01, 0, 0, 0, 0]));
    assert_eq!(
        response_rops
            .windows(6)
            .filter(|window| *window == [0x0C, 0x01, 0, 0, 0, 0])
            .count(),
        2,
        "both the mutating and no-op Save must succeed: {response_rops:02x?}"
    );
    let expected_notification_folder_id = identity_codec
        .wire_id_bytes_from_object_id(advertised_target.0)
        .unwrap();
    let expected_notification_message_id = identity_codec
        .wire_id_bytes_from_object_id(advertised_target.1)
        .unwrap();
    let object_modified_notifications = response_rops
        .windows(26)
        .enumerate()
        .filter_map(|(offset, window)| {
            (window[0] == 0x2A
                && window[5] == 0
                && window[6..8] == [0x10, 0x80]
                && window[8..16] == expected_notification_folder_id
                && window[16..24] == expected_notification_message_id
                && window[24..26] == [0, 0])
            .then_some(offset)
        })
        .collect::<Vec<_>>();
    assert_eq!(
        object_modified_notifications.len(),
        1,
        "the committed LocalFreebusy mutation must emit one message ObjectModified: {response_rops:02x?}"
    );
    let notify_offset = object_modified_notifications[0];
    assert_eq!(
        &response_rops[notify_offset + 8..notify_offset + 16],
        &expected_notification_folder_id
    );
    assert_eq!(
        &response_rops[notify_offset + 16..notify_offset + 24],
        &expected_notification_message_id
    );
    assert_eq!(
        &response_rops[notify_offset + 24..notify_offset + 26],
        &0u16.to_le_bytes(),
        "[MS-OXCNOTIF] section 2.2.1.4.1.2 ObjectModified TagCount"
    );
    renew_mapi_request_id(&mut observer_execute_headers);
    let observer_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &observer_execute_headers,
            &execute_body(&rop_buffer(&[], &[])),
        )
        .await
        .unwrap();
    let observer_response_rops = response_rops_from_execute_response(observer_response).await;
    let observer_notifications = observer_response_rops
        .windows(26)
        .filter(|window| {
            window[0] == 0x2A
                && window[5] == 0
                && window[6..8] == [0x10, 0x80]
                && window[8..16] == expected_notification_folder_id
                && window[16..24] == expected_notification_message_id
                && window[24..26] == [0, 0]
        })
        .count();
    assert_eq!(
        observer_notifications, 1,
        "the other live session must replay exactly one durable LocalFreebusy ObjectModified: {observer_response_rops:02x?}"
    );
    let row_offset = mapi_get_properties_specific_standard_row_offset(&response_rops, 1)
        .expect("saved LocalFreebusy custom and identity GetProps");
    let mut saved_value_offset = row_offset + 1;
    assert_eq!(response_rops[saved_value_offset], 1);
    saved_value_offset += 1;
    let response_source_key = read_rop_binary_u16(&response_rops, &mut saved_value_offset)
        .map_err(anyhow::Error::msg)?
        .to_vec();
    let response_change_key = read_rop_binary_u16(&response_rops, &mut saved_value_offset)
        .map_err(anyhow::Error::msg)?
        .to_vec();
    let response_predecessor_change_list =
        read_rop_binary_u16(&response_rops, &mut saved_value_offset)
            .map_err(anyhow::Error::msg)?
            .to_vec();
    let response_change_number = crate::mapi::identity::object_id_from_wire_id(
        &response_rops[saved_value_offset..saved_value_offset + 8],
    )
    .and_then(crate::mapi::identity::global_counter_from_store_id)
    .expect("saved LocalFreebusy ChangeNumber wire id");
    saved_value_offset += 8;
    let response_last_modification_time = u64::from_le_bytes(
        response_rops[saved_value_offset..saved_value_offset + 8]
            .try_into()
            .unwrap(),
    );
    let saved_snapshot = storage
        .load_mapi_mail_store(delegate_account_id, 500)
        .await?;
    let saved_local_freebusy = saved_snapshot
        .delegate_freebusy_messages()
        .iter()
        .find(|message| crate::mapi_store::is_outlook_local_freebusy_message(message))
        .expect("saved delegate snapshot contains LocalFreebusy");
    let saved_identity = saved_local_freebusy
        .durable_identity
        .clone()
        .expect("saved LocalFreebusy has a durable MAPI identity");
    assert_eq!(saved_identity.object_id, local_freebusy_identity.object_id);
    assert_eq!(
        saved_identity.source_key,
        local_freebusy_identity.source_key
    );
    assert!(saved_identity.change_number > local_freebusy_identity.change_number);
    assert_ne!(
        saved_identity.change_key,
        local_freebusy_identity.change_key
    );
    assert!(saved_identity.last_modification_time > local_freebusy_identity.last_modification_time);
    assert!(test_mapi_pcl_includes_change_key(
        &saved_identity.predecessor_change_list,
        &local_freebusy_identity.change_key
    ));
    assert!(test_mapi_pcl_includes_change_key(
        &saved_identity.predecessor_change_list,
        &saved_identity.change_key
    ));
    assert_eq!(response_source_key, saved_identity.source_key);
    assert_eq!(response_change_key, saved_identity.change_key);
    assert_eq!(
        response_predecessor_change_list,
        saved_identity.predecessor_change_list
    );
    assert_eq!(response_change_number, saved_identity.change_number);
    assert_eq!(
        response_last_modification_time,
        saved_identity.last_modification_time
    );
    assert_eq!(
        saved_local_freebusy.custom_properties,
        vec![crate::store::MapiCustomPropertyValue {
            property_tag: fixupfbfolder_tag,
            property_type: 0x000B,
            property_value: vec![1],
        }]
    );
    let projection_state_after = sqlx::query_as::<_, (i64, i64)>(
        r#"
        SELECT revision, applied_revision
        FROM delegation_projection_state
        WHERE tenant_id = $1 AND account_id = $2
        "#,
    )
    .bind(tenant_id)
    .bind(delegate_account_id)
    .fetch_one(storage.pool())
    .await?;
    assert_eq!(projection_state_after, projection_state_before);

    renew_mapi_request_id(&mut execute_headers);
    let mut reopened_fixup_rops = Vec::new();
    append_rop_open_message(
        &mut reopened_fixup_rops,
        0,
        1,
        advertised_target.0,
        advertised_target.1,
    );
    append_rop_get_properties_specific(&mut reopened_fixup_rops, 1, &[fixupfbfolder_tag]);
    reopened_fixup_rops.extend_from_slice(&[0x08, 0x00, 0x01, 0x00, 0x10, 0x01, 0x00]);
    reopened_fixup_rops.extend_from_slice(&[0x09, 0x00, 0x01]);
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&reopened_fixup_rops, &[logon_handle, u32::MAX])),
        )
        .await
        .unwrap();
    let response_rops = response_rops_from_execute_response(response).await;
    let reopened_open = response_rops
        .windows(6)
        .position(|window| window == [0x03, 0x01, 0, 0, 0, 0])
        .expect("reopened LocalFreebusy OpenMessage");
    assert_eq!(
        response_rops[reopened_open + 6],
        1,
        "OpenMessage HasNamedProperties must reflect the persisted custom bag"
    );
    let row_offset = mapi_get_properties_specific_standard_row_offset(&response_rops, 1)
        .expect("reopened LocalFreebusy fixupfbfolder GetProps");
    assert_eq!(response_rops[row_offset + 1], 1);
    let get_all_start = response_rops
        .windows(6)
        .position(|window| window == [0x08, 0x01, 0, 0, 0, 0])
        .expect("persisted LocalFreebusy GetPropertiesAll");
    let get_list_start = response_rops[get_all_start + 6..]
        .windows(6)
        .position(|window| window == [0x09, 0x01, 0, 0, 0, 0])
        .map(|offset| get_all_start + 6 + offset)
        .expect("persisted LocalFreebusy GetPropertiesList");
    assert!(contains_bytes(
        &response_rops[get_all_start..get_list_start],
        &expected_fixup_property
    ));
    assert!(contains_bytes(
        &response_rops[get_list_start..],
        &fixupfbfolder_tag.to_le_bytes()
    ));

    // [MS-OXCPRPT] section 3.2.5.4: both property-copy ROPs participate in
    // the destination Message transaction. Copy the source handle's pending
    // value into two LocalFreebusy handles, then release all three without a
    // Save; neither operation may publish the copied value.
    renew_mapi_request_id(&mut execute_headers);
    let mut transactional_copy_rops = Vec::new();
    for output in 1..=3 {
        append_rop_open_message(
            &mut transactional_copy_rops,
            0,
            output,
            advertised_target.0,
            advertised_target.1,
        );
    }
    let mut false_fixup_value = Vec::new();
    append_mapi_bool_property(&mut false_fixup_value, fixupfbfolder_tag, false);
    append_rop_set_properties(&mut transactional_copy_rops, 1, 1, &false_fixup_value);
    transactional_copy_rops.extend_from_slice(&[
        0x67, 0x00, 0x01, 0x02, // RopCopyProperties: handle 1 -> handle 2.
        0x00, 0x00, // WantAsynchronous, CopyFlags.
    ]);
    transactional_copy_rops.extend_from_slice(&1u16.to_le_bytes());
    transactional_copy_rops.extend_from_slice(&fixupfbfolder_tag.to_le_bytes());
    transactional_copy_rops.extend_from_slice(&[
        0x39, 0x00, 0x01, 0x03, // RopCopyTo: handle 1 -> handle 3.
        0x00, 0x00, 0x00, // WantAsynchronous, WantSubObjects, CopyFlags.
    ]);
    transactional_copy_rops.extend_from_slice(&0u16.to_le_bytes());
    append_rop_get_properties_specific(&mut transactional_copy_rops, 2, &[fixupfbfolder_tag]);
    append_rop_get_properties_specific(&mut transactional_copy_rops, 3, &[fixupfbfolder_tag]);
    transactional_copy_rops.extend_from_slice(&[
        0x01, 0x00, 0x01, // Release source.
        0x01, 0x00, 0x02, // Release CopyProperties destination.
        0x01, 0x00, 0x03, // Release CopyTo destination.
    ]);
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(
                &transactional_copy_rops,
                &[logon_handle, u32::MAX, u32::MAX, u32::MAX],
            )),
        )
        .await
        .unwrap();
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(
        &response_rops,
        &[0x67, 0x01, 0, 0, 0, 0, 0, 0]
    ));
    assert!(contains_bytes(
        &response_rops,
        &[0x39, 0x01, 0, 0, 0, 0, 0, 0]
    ));
    for handle_index in [2, 3] {
        let row = mapi_get_properties_specific_standard_row_offset(&response_rops, handle_index)
            .expect("copied LocalFreebusy handle sees its pending custom value");
        assert_eq!(&response_rops[row..row + 2], &[0, 0]);
    }
    let durable_fixup_value = sqlx::query_scalar::<_, Vec<u8>>(
        r#"
        SELECT property_value
        FROM mapi_custom_property_values
        WHERE tenant_id = $1
          AND account_id = $2
          AND object_kind = 'delegate_freebusy_message'
          AND canonical_id = $3
          AND property_tag = $4
        "#,
    )
    .bind(tenant_id)
    .bind(delegate_account_id)
    .bind(crate::mapi_store::OUTLOOK_LOCAL_FREEBUSY_CANONICAL_ID)
    .bind(i64::from(fixupfbfolder_tag))
    .fetch_one(storage.pool())
    .await?;
    assert_eq!(durable_fixup_value, vec![1]);

    renew_mapi_request_id(&mut execute_headers);
    let mut copy_rops = Vec::new();
    append_rop_open_message(
        &mut copy_rops,
        0,
        1,
        advertised_target.0,
        advertised_target.1,
    );
    copy_rops.extend_from_slice(&[0x4D, 0x00, 0x01, 0x02]);
    copy_rops.push(0);
    copy_rops.extend_from_slice(&0x0000_2000u32.to_le_bytes());
    copy_rops.push(0x09);
    copy_rops.extend_from_slice(&0u16.to_le_bytes());
    copy_rops.extend_from_slice(&[0x4E, 0x00, 0x02]);
    copy_rops.extend_from_slice(&4096u16.to_le_bytes());
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&copy_rops, &[logon_handle, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    let response_rops = response_rops_from_execute_response(response).await;
    let chunks = mapi_fast_transfer_chunks(&response_rops);
    assert_eq!(chunks.len(), 1, "{response_rops:02x?}");
    assert_eq!(chunks[0].0, 0x0003, "{response_rops:02x?}");
    let transfer = &chunks[0].1;
    assert_eq!(
        mapi_last_binary_property(transfer, PID_TAG_SOURCE_KEY),
        Some(saved_identity.source_key.as_slice())
    );
    assert_eq!(
        mapi_last_binary_property(transfer, PID_TAG_CHANGE_KEY),
        Some(saved_identity.change_key.as_slice())
    );
    let fixup_property_offset = transfer
        .windows(4)
        .position(|window| window == fixupfbfolder_tag.to_le_bytes())
        .expect("LocalFreebusy CopyTo includes persisted fixupfbfolder");
    assert_eq!(
        &transfer[fixup_property_offset + 4..fixup_property_offset + 20],
        &ps_public_strings
    );
    assert_eq!(transfer[fixup_property_offset + 20], 0x01);
    let fixup_name_start = fixup_property_offset + 21;
    assert_eq!(
        &transfer[fixup_name_start..fixup_name_start + fixupfbfolder_name.len()],
        &fixupfbfolder_name
    );
    assert_eq!(
        &transfer[fixup_name_start + fixupfbfolder_name.len()
            ..fixup_name_start + fixupfbfolder_name.len() + 2],
        &[1, 0]
    );
    for provider_local_tag in [PID_TAG_ENTRY_ID, 0x0FF6_0102, 0x0E0B_0102] {
        assert!(
            !contains_bytes(transfer, &provider_local_tag.to_le_bytes()),
            "LocalFreebusy direct CopyTo must omit provider-local property {provider_local_tag:#010x}"
        );
    }

    // DeleteProperties has the same Message transaction boundary. The
    // deleting handle hides the stored value immediately, an independently
    // opened handle retains the durable value, and Save publishes deletion.
    renew_mapi_request_id(&mut execute_headers);
    let mut delete_fixup_rops = Vec::new();
    append_rop_open_message(
        &mut delete_fixup_rops,
        0,
        1,
        advertised_target.0,
        advertised_target.1,
    );
    append_rop_delete_properties(&mut delete_fixup_rops, 1, &[fixupfbfolder_tag]);
    append_rop_get_properties_specific(&mut delete_fixup_rops, 1, &[fixupfbfolder_tag]);
    delete_fixup_rops.extend_from_slice(&[0x08, 0x00, 0x01, 0x00, 0x10, 0x01, 0x00]);
    delete_fixup_rops.extend_from_slice(&[0x09, 0x00, 0x01]);
    append_rop_open_message(
        &mut delete_fixup_rops,
        0,
        2,
        advertised_target.0,
        advertised_target.1,
    );
    append_rop_get_properties_specific(&mut delete_fixup_rops, 2, &[fixupfbfolder_tag]);
    append_rop_save_changes_message_with_flags(&mut delete_fixup_rops, 1, 1, 0x0A);
    append_rop_get_properties_specific(&mut delete_fixup_rops, 1, &[fixupfbfolder_tag]);
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(
                &delete_fixup_rops,
                &[logon_handle, u32::MAX, u32::MAX],
            )),
        )
        .await
        .unwrap();
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(&response_rops, &[0x0B, 0x01, 0, 0, 0, 0]));
    assert!(contains_bytes(&response_rops, &[0x0C, 0x01, 0, 0, 0, 0]));
    let first_handle_marker = [0x07, 0x01, 0, 0, 0, 0];
    let first_handle_rows = response_rops
        .windows(first_handle_marker.len())
        .enumerate()
        .filter_map(|(offset, window)| {
            (window == first_handle_marker).then_some(offset + first_handle_marker.len())
        })
        .collect::<Vec<_>>();
    assert_eq!(first_handle_rows.len(), 2);
    for row in first_handle_rows {
        assert_eq!(response_rops[row], 1);
        assert_eq!(response_rops[row + 1], 0x0A);
        assert_eq!(
            u32::from_le_bytes(response_rops[row + 2..row + 6].try_into().unwrap()),
            0x8004_010F
        );
    }
    let delete_get_all_start = response_rops
        .windows(6)
        .position(|window| window == [0x08, 0x01, 0, 0, 0, 0])
        .expect("deleted LocalFreebusy GetPropertiesAll");
    let delete_get_list_start = response_rops[delete_get_all_start + 6..]
        .windows(6)
        .position(|window| window == [0x09, 0x01, 0, 0, 0, 0])
        .map(|offset| delete_get_all_start + 6 + offset)
        .expect("deleted LocalFreebusy GetPropertiesList");
    let delete_second_open = response_rops[delete_get_list_start + 6..]
        .windows(6)
        .position(|window| window == [0x03, 0x02, 0, 0, 0, 0])
        .map(|offset| delete_get_list_start + 6 + offset)
        .expect("second LocalFreebusy OpenMessage during deletion");
    assert!(!contains_bytes(
        &response_rops[delete_get_all_start..delete_get_list_start],
        &fixupfbfolder_tag.to_le_bytes()
    ));
    assert!(!contains_bytes(
        &response_rops[delete_get_list_start..delete_second_open],
        &fixupfbfolder_tag.to_le_bytes()
    ));
    let other_handle_row = mapi_get_properties_specific_standard_row_offset(&response_rops, 2)
        .expect("other LocalFreebusy handle retains the durable custom value");
    assert_eq!(response_rops[other_handle_row + 1], 1);

    let reloaded_snapshot = storage
        .load_mapi_mail_store(delegate_account_id, 500)
        .await?;
    let reloaded_local_freebusy = reloaded_snapshot
        .delegate_freebusy_messages()
        .iter()
        .find(|message| crate::mapi_store::is_outlook_local_freebusy_message(message))
        .expect("reloaded delegate snapshot contains LocalFreebusy");
    assert_eq!(reloaded_local_freebusy.id, local_freebusy_id);
    let deleted_identity = reloaded_local_freebusy
        .durable_identity
        .as_ref()
        .expect("reloaded LocalFreebusy has a durable identity");
    assert_eq!(deleted_identity.object_id, saved_identity.object_id);
    assert_eq!(deleted_identity.source_key, saved_identity.source_key);
    assert!(deleted_identity.change_number > saved_identity.change_number);
    assert_ne!(deleted_identity.change_key, saved_identity.change_key);
    assert!(deleted_identity.last_modification_time > saved_identity.last_modification_time);
    assert!(test_mapi_pcl_includes_change_key(
        &deleted_identity.predecessor_change_list,
        &saved_identity.change_key
    ));
    assert!(test_mapi_pcl_includes_change_key(
        &deleted_identity.predecessor_change_list,
        &deleted_identity.change_key
    ));
    assert!(reloaded_local_freebusy.custom_properties.is_empty());
    let deleted_row_count = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*)
        FROM mapi_custom_property_values
        WHERE tenant_id = $1
          AND account_id = $2
          AND object_kind = 'delegate_freebusy_message'
          AND canonical_id = $3
          AND property_tag = $4
        "#,
    )
    .bind(tenant_id)
    .bind(delegate_account_id)
    .bind(crate::mapi_store::OUTLOOK_LOCAL_FREEBUSY_CANONICAL_ID)
    .bind(i64::from(fixupfbfolder_tag))
    .fetch_one(storage.pool())
    .await?;
    assert_eq!(deleted_row_count, 0);

    renew_mapi_request_id(&mut execute_headers);
    let mut reopened_deleted_rops = Vec::new();
    append_rop_open_message(
        &mut reopened_deleted_rops,
        0,
        1,
        advertised_target.0,
        advertised_target.1,
    );
    append_rop_get_properties_specific(&mut reopened_deleted_rops, 1, &[fixupfbfolder_tag]);
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(
                &reopened_deleted_rops,
                &[logon_handle, u32::MAX],
            )),
        )
        .await
        .unwrap();
    let response_rops = response_rops_from_execute_response(response).await;
    let deleted_marker = [0x07, 0x01, 0, 0, 0, 0];
    let deleted_row = response_rops
        .windows(deleted_marker.len())
        .position(|window| window == deleted_marker)
        .map(|offset| offset + deleted_marker.len())
        .expect("reopened deleted LocalFreebusy fixupfbfolder GetProps");
    assert_eq!(response_rops[deleted_row], 1);
    assert_eq!(response_rops[deleted_row + 1], 0x0A);
    assert_eq!(
        u32::from_le_bytes(
            response_rops[deleted_row + 2..deleted_row + 6]
                .try_into()
                .unwrap()
        ),
        0x8004_010F
    );

    renew_mapi_request_id(&mut execute_headers);
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&get_rops, &[logon_handle, u32::MAX])),
        )
        .await
        .unwrap();
    let reloaded_response_rops = response_rops_from_execute_response(response).await;
    let get_offset = reloaded_response_rops
        .windows(marker.len())
        .position(|window| window == marker)
        .expect("reloaded LocalFreebusy raw548 GetProps response");
    let mut value_offset = get_offset + marker.len();
    assert_eq!(reloaded_response_rops[value_offset], 1);
    value_offset += 1;
    for property_tag in EXCHANGE_RAW548_TAGS.iter().take(14) {
        assert_eq!(
            reloaded_response_rops[value_offset], 0x0A,
            "reloaded LocalFreebusy {property_tag:#010x} must remain absent"
        );
        value_offset += 1;
        assert_eq!(
            u32::from_le_bytes(
                reloaded_response_rops[value_offset..value_offset + 4]
                    .try_into()
                    .unwrap()
            ),
            0x8004_010F
        );
        value_offset += 4;
    }
    assert_eq!(reloaded_response_rops[value_offset], 0);
    value_offset += 1;
    assert_eq!(
        u64::from_le_bytes(
            reloaded_response_rops[value_offset..value_offset + 8]
                .try_into()
                .unwrap()
        ),
        deleted_identity.last_modification_time
    );
    value_offset += 8;
    assert_eq!(reloaded_response_rops[value_offset], 0);
    value_offset += 1;
    assert_eq!(
        read_rop_binary_u16(&reloaded_response_rops, &mut value_offset).unwrap(),
        deleted_identity.change_key
    );
    assert_eq!(reloaded_response_rops[value_offset], 0);
    value_offset += 1;
    assert_eq!(
        read_rop_binary_u16(&reloaded_response_rops, &mut value_offset).unwrap(),
        crate::mapi::identity::outlook_message_list_settings_entry_id(
            delegate_account_id,
            local_freebusy_id,
        )
        .unwrap()
    );
    assert_eq!(reloaded_response_rops[value_offset], 0);
    value_offset += 1;
    assert_eq!(
        read_rop_utf16z(&reloaded_response_rops, &mut value_offset).unwrap(),
        "IPM.Microsoft.ScheduleData.FreeBusy"
    );

    let owner_snapshot = storage
        .load_mapi_mail_store(fixture.account_id, 500)
        .await?;
    let owner_local_freebusy = owner_snapshot
        .delegate_freebusy_messages()
        .iter()
        .find(|message| crate::mapi_store::is_outlook_local_freebusy_message(message))
        .expect("owner snapshot contains LocalFreebusy");
    assert_eq!(owner_local_freebusy.delegates.len(), 1);
    assert_eq!(
        owner_local_freebusy.delegates[0].grantee_account_id,
        delegate_account_id
    );
    let owner_service = ExchangeService::new(storage.clone());
    let (owner_headers, owner_logon_handle) = mapi_connect_with_private_logon(&owner_service).await;
    let mut owner_get_rops = Vec::new();
    append_rop_open_message(
        &mut owner_get_rops,
        0,
        1,
        crate::mapi::identity::FREEBUSY_DATA_FOLDER_ID,
        owner_local_freebusy.id,
    );
    append_rop_get_properties_specific(&mut owner_get_rops, 1, &EXCHANGE_RAW548_TAGS[..14]);
    let response = owner_service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &owner_headers,
            &execute_body(&rop_buffer(
                &owner_get_rops,
                &[owner_logon_handle, u32::MAX],
            )),
        )
        .await
        .unwrap();
    let response_rops = response_rops_from_execute_response(response).await;
    let get_offset = response_rops
        .windows(marker.len())
        .position(|window| window == marker)
        .expect("configured owner LocalFreebusy GetProps response");
    let mut value_offset = get_offset + marker.len();
    assert_eq!(response_rops[value_offset], 1);
    value_offset += 1;

    assert_eq!(response_rops[value_offset], 0x0A); // 0x6841 remains unsupported.
    value_offset += 1;
    assert_eq!(
        u32::from_le_bytes(
            response_rops[value_offset..value_offset + 4]
                .try_into()
                .unwrap()
        ),
        0x8004_010F
    );
    value_offset += 4;
    assert_eq!(response_rops[value_offset..value_offset + 2], [0, 0]); // WantsCopy=false.
    value_offset += 2;
    assert_eq!(response_rops[value_offset..value_offset + 2], [0, 1]); // DontMail=true.
    value_offset += 2;

    assert_eq!(response_rops[value_offset], 0);
    value_offset += 1;
    assert_eq!(
        u32::from_le_bytes(
            response_rops[value_offset..value_offset + 4]
                .try_into()
                .unwrap()
        ),
        1
    );
    value_offset += 4;
    assert_eq!(
        read_rop_utf16z(&response_rops, &mut value_offset).unwrap(),
        "Delegate User"
    );

    assert_eq!(response_rops[value_offset], 0);
    value_offset += 1;
    assert_eq!(
        u32::from_le_bytes(
            response_rops[value_offset..value_offset + 4]
                .try_into()
                .unwrap()
        ),
        1
    );
    value_offset += 4;
    let delegate_entry_id = read_rop_binary_u16(&response_rops, &mut value_offset).unwrap();
    assert_eq!(&delegate_entry_id[..4], &[0, 0, 0, 0]);
    assert!(delegate_entry_id.ends_with(
        b"/o=LPE/ou=Exchange Administrative Group/cn=Recipients/cn=delegate-example-test\0"
    ));

    assert_eq!(response_rops[value_offset], 0);
    value_offset += 1;
    assert_eq!(
        u32::from_le_bytes(
            response_rops[value_offset..value_offset + 4]
                .try_into()
                .unwrap()
        ),
        1
    );
    value_offset += 4;
    assert_eq!(
        i32::from_le_bytes(
            response_rops[value_offset..value_offset + 4]
                .try_into()
                .unwrap()
        ),
        1
    );
    value_offset += 4;

    for property_tag in EXCHANGE_RAW548_TAGS.iter().take(12).skip(6) {
        assert_eq!(
            response_rops[value_offset], 0x0A,
            "unsupported configured delegate property {property_tag:#010x} must stay absent"
        );
        value_offset += 1;
        assert_eq!(
            u32::from_le_bytes(
                response_rops[value_offset..value_offset + 4]
                    .try_into()
                    .unwrap()
            ),
            0x8004_010F
        );
        value_offset += 4;
    }
    assert_eq!(response_rops[value_offset..value_offset + 2], [0, 0]); // WantsInfo=false.
    value_offset += 2;
    assert_eq!(response_rops[value_offset], 0);
    value_offset += 1;
    assert_eq!(
        u32::from_le_bytes(
            response_rops[value_offset..value_offset + 4]
                .try_into()
                .unwrap()
        ),
        1
    );
    value_offset += 4;
    assert_eq!(
        read_rop_utf16z(&response_rops, &mut value_offset).unwrap(),
        "Delegate User"
    );

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
    let (execute_headers, logon_handle) = mapi_connect_with_private_logon(&service).await;

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
            &execute_body(&rop_buffer(&rops, &[logon_handle, u32::MAX, u32::MAX])),
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
    let (execute_headers, logon_handle) = mapi_connect_with_private_logon(&service).await;

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
            &execute_body(&rop_buffer(&rops, &[logon_handle, u32::MAX, u32::MAX])),
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
    let delegate_member_id = crate::mapi::identity::mapi_store_id(0x150);
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
    let (execute_headers, logon_handle) = mapi_connect_with_private_logon(&service).await;

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
            &execute_body(&rop_buffer(&rops, &[logon_handle, u32::MAX])),
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
    let delegate_member_id = crate::mapi::identity::mapi_store_id(0x151);
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
    let (execute_headers, logon_handle) = mapi_connect_with_private_logon(&service).await;

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
            &execute_body(&rop_buffer(&rops, &[logon_handle, u32::MAX])),
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
    let delegate_member_id = crate::mapi::identity::mapi_store_id(0x152);
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
    let (execute_headers, logon_handle) = mapi_connect_with_private_logon(&service).await;

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
            &execute_body(&rop_buffer(&rops, &[logon_handle, u32::MAX])),
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
    let delegate_member_id = crate::mapi::identity::mapi_store_id(0x153);
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
    let (execute_headers, logon_handle) = mapi_connect_with_private_logon(&service).await;

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
            &execute_body(&rop_buffer(&rops, &[logon_handle, u32::MAX])),
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
    let delegate_member_id = crate::mapi::identity::mapi_store_id(0x154);
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
    let (execute_headers, logon_handle) = mapi_connect_with_private_logon(&service).await;

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
            &execute_body(&rop_buffer(&rops, &[logon_handle, u32::MAX])),
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
    let delegate_member_id = crate::mapi::identity::mapi_store_id(0x155);
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
    let (execute_headers, logon_handle) = mapi_connect_with_private_logon(&service).await;

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
            &execute_body(&rop_buffer(&rops, &[logon_handle, u32::MAX])),
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
    let (execute_headers, logon_handle) = mapi_connect_with_private_logon(&service).await;

    let mut rops = vec![0x02, 0x00, 0x00, 0x01];
    append_mapi_wire_id(&mut rops, test_mapi_folder_id(5));
    rops.push(0);
    append_rop_create_message(&mut rops, 1, 2, test_mapi_folder_id(5));

    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[logon_handle, u32::MAX, u32::MAX])),
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
    let (execute_headers, logon_handle) = mapi_connect_with_private_logon(&service).await;

    let mut rops = vec![0x02, 0x00, 0x00, 0x01];
    append_mapi_wire_id(&mut rops, test_mapi_folder_id(5));
    rops.push(0);
    rops.extend_from_slice(&[0x05, 0x00, 0x01, 0x02, 0x00]);

    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[logon_handle, u32::MAX, u32::MAX])),
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
    let (execute_headers, logon_handle) = mapi_connect_with_private_logon(&service).await;

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
                    logon_handle,
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
