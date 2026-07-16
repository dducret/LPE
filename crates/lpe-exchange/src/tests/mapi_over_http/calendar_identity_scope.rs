use super::*;

fn scoped_identity_event_input(
    account_id: Uuid,
    event_id: Uuid,
    collection_id: &str,
) -> UpsertClientEventInput {
    UpsertClientEventInput {
        id: Some(event_id),
        account_id,
        uid: "mapi-calendar-principal-identity-scope".to_string(),
        date: "2026-07-15".to_string(),
        time: "10:15".to_string(),
        time_zone: "Europe/Berlin".to_string(),
        duration_minutes: 45,
        all_day: false,
        status: "confirmed".to_string(),
        sequence: 0,
        recurrence_rule: String::new(),
        recurrence_json: "{}".to_string(),
        recurrence_exceptions_json: "[]".to_string(),
        title: "Principal-scoped Calendar identity".to_string(),
        location: collection_id.to_string(),
        organizer_json:
            r#"{"email":"alice@example.test","common_name":"Alice Calendar"}"#.to_string(),
        attendees: "identity-grantee@example.test".to_string(),
        attendees_json: r#"{"attendees":[{"email":"identity-grantee@example.test","common_name":"Identity Grantee","role":"REQ-PARTICIPANT","partstat":"accepted","rsvp":false}]}"#.to_string(),
        notes: "Request-scoped FID, MID, and SourceKey regression".to_string(),
        body_html: "<p>Request-scoped FID, MID, and SourceKey regression</p>".to_string(),
    }
}

async fn insert_calendar_identity_account(
    storage: &Storage,
    owner_account_id: Uuid,
    account_id: Uuid,
) -> anyhow::Result<()> {
    sqlx::query(
        r#"
        INSERT INTO accounts (
            id, tenant_id, primary_domain_id, primary_email, display_name
        )
        SELECT $1, tenant_id, primary_domain_id, $2, $3
        FROM accounts
        WHERE id = $4
        "#,
    )
    .bind(account_id)
    .bind("identity-grantee@example.test")
    .bind("Identity Grantee")
    .bind(owner_account_id)
    .execute(storage.pool())
    .await?;
    Ok(())
}

#[tokio::test]
async fn mapi_calendar_snapshot_identity_is_principal_scoped_in_postgresql() -> anyhow::Result<()> {
    let Some(fixture) = postgres_mapi_calendar_fixture().await? else {
        return Ok(());
    };
    let storage = fixture.storage.clone();
    let owner_account_id = fixture.account_id;
    let grantee_account_id = Uuid::parse_str("10000000-0000-0000-0000-000000000020")?;
    insert_calendar_identity_account(&storage, owner_account_id, grantee_account_id).await?;

    let collection = storage
        .create_accessible_calendar_collection(owner_account_id, "Scoped Calendar Identity Lab")
        .await?;
    let calendar_id = Uuid::parse_str(&collection.id)?;
    storage
        .upsert_collaboration_grant(
            CollaborationGrantInput {
                kind: CollaborationResourceKind::Calendar,
                owner_account_id,
                grantee_email: "identity-grantee@example.test".to_string(),
                calendar_id: Some(calendar_id),
                may_read: true,
                may_write: false,
                may_delete: false,
                may_share: false,
            },
            lpe_storage::AuditEntryInput {
                actor: "alice@example.test".to_string(),
                action: "test-mapi-calendar-principal-identity-scope".to_string(),
                subject: calendar_id.to_string(),
            },
        )
        .await?;

    let event_id = Uuid::parse_str("82828282-8282-4282-9282-828282828282")?;
    storage
        .create_accessible_event(
            owner_account_id,
            Some(&collection.id),
            scoped_identity_event_input(owner_account_id, event_id, &collection.id),
        )
        .await?;

    let owner_snapshot = storage.load_mapi_mail_store(owner_account_id, 500).await?;
    let owner_folder_id = owner_snapshot
        .collaboration_folders()
        .iter()
        .find(|folder| folder.collection.id == collection.id)
        .map(|folder| folder.id)
        .ok_or_else(|| anyhow::anyhow!("owner custom Calendar folder was not projected"))?;
    let owner_event = owner_snapshot
        .events_for_folder(owner_folder_id)
        .into_iter()
        .find(|event| event.canonical_id == event_id)
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("owner Calendar event was not projected"))?;
    let owner_identity = storage
        .fetch_or_allocate_mapi_identities(
            owner_account_id,
            &[MapiIdentityRequest {
                object_kind: MapiIdentityObjectKind::CalendarEvent,
                canonical_id: event_id,
                reserved_global_counter: None,
                source_key: None,
            }],
        )
        .await?
        .into_iter()
        .next()
        .ok_or_else(|| anyhow::anyhow!("owner Calendar Event identity was not persisted"))?;
    assert_eq!(owner_event.id, owner_identity.object_id);
    assert_eq!(owner_event.source_key, owner_identity.source_key);

    let skew_requests = (0u128..64)
        .map(|offset| MapiIdentityRequest {
            object_kind: MapiIdentityObjectKind::Account,
            canonical_id: Uuid::from_u128(0x9300_0000_0000_0000_0000_0000_0000_0000 + offset),
            reserved_global_counter: None,
            source_key: None,
        })
        .collect::<Vec<_>>();
    storage
        .fetch_or_allocate_mapi_identities(grantee_account_id, &skew_requests)
        .await?;

    let grantee_snapshot = storage
        .load_mapi_mail_store(grantee_account_id, 500)
        .await?;
    let grantee_folder_id = grantee_snapshot
        .collaboration_folders()
        .iter()
        .find(|folder| folder.collection.id == collection.id)
        .map(|folder| folder.id)
        .ok_or_else(|| anyhow::anyhow!("grantee custom Calendar folder was not projected"))?;
    let grantee_event = grantee_snapshot
        .events_for_folder(grantee_folder_id)
        .into_iter()
        .find(|event| event.canonical_id == event_id)
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("grantee Calendar event was not projected"))?;
    let grantee_identity = storage
        .fetch_or_allocate_mapi_identities(
            grantee_account_id,
            &[MapiIdentityRequest {
                object_kind: MapiIdentityObjectKind::CalendarEvent,
                canonical_id: event_id,
                reserved_global_counter: None,
                source_key: None,
            }],
        )
        .await?
        .into_iter()
        .next()
        .ok_or_else(|| anyhow::anyhow!("grantee Calendar Event identity was not persisted"))?;

    assert_ne!(owner_folder_id, grantee_folder_id);
    assert_ne!(owner_event.id, grantee_event.id);
    assert_ne!(owner_event.source_key, grantee_event.source_key);
    assert_eq!(grantee_event.id, grantee_identity.object_id);
    assert_eq!(grantee_event.source_key, grantee_identity.source_key);

    let owner_event_after_grantee_load = owner_snapshot
        .event_for_id(owner_folder_id, owner_event.id)
        .ok_or_else(|| anyhow::anyhow!("owner MID stopped resolving after grantee load"))?;
    assert_eq!(owner_event_after_grantee_load.id, owner_identity.object_id);
    assert_eq!(
        owner_event_after_grantee_load.source_key,
        owner_identity.source_key
    );
    assert!(owner_snapshot
        .event_for_id(owner_folder_id, grantee_event.id)
        .is_none());

    fixture.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn mapi_identity_repair_preserves_rotated_calendar_change_key() -> anyhow::Result<()> {
    let Some(fixture) = postgres_mapi_calendar_fixture().await? else {
        return Ok(());
    };
    let storage = fixture.storage.clone();
    let account_id = fixture.account_id;
    let collection = storage
        .create_accessible_calendar_collection(account_id, "ChangeKey repair regression")
        .await?;
    let event_id = Uuid::parse_str("83838383-8383-4383-9383-838383838383")?;
    storage
        .create_accessible_event(
            account_id,
            Some(&collection.id),
            scoped_identity_event_input(account_id, event_id, &collection.id),
        )
        .await?;

    let identity = storage
        .fetch_or_allocate_mapi_identities(
            account_id,
            &[MapiIdentityRequest {
                object_kind: MapiIdentityObjectKind::CalendarEvent,
                canonical_id: event_id,
                reserved_global_counter: None,
                source_key: None,
            }],
        )
        .await?
        .remove(0);
    let global_counter =
        crate::mapi::identity::global_counter_from_store_id(identity.object_id).unwrap();
    let rotated_change_number = global_counter + 1;
    let rotated_change_key =
        crate::mapi::identity::change_key_for_change_number(rotated_change_number);
    let rotated_predecessor_list = mapi_mailstore::predecessor_change_list(rotated_change_number);
    sqlx::query(
        r#"
        UPDATE mapi_object_identities
        SET mapi_change_number = $3,
            change_key = $4,
            predecessor_change_list = $5
        WHERE account_id = $1
          AND object_kind = 'calendar_event'
          AND canonical_id = $2
        "#,
    )
    .bind(account_id)
    .bind(event_id)
    .bind(rotated_change_number as i64)
    .bind(&rotated_change_key)
    .bind(&rotated_predecessor_list)
    .execute(storage.pool())
    .await?;

    // Reopening the identity runs the repair helper that regressed Test 08:34
    // from CN 71 to its immutable object counter 70. [MS-OXCFXICS] sections
    // 2.2.1.2.7, 2.2.1.2.8, and 3.1.5.3 require the rotated CK/CN/PCL
    // version to remain coherent across reopen and synchronization.
    storage
        .fetch_or_allocate_mapi_identities(
            account_id,
            &[MapiIdentityRequest {
                object_kind: MapiIdentityObjectKind::CalendarEvent,
                canonical_id: event_id,
                reserved_global_counter: None,
                source_key: None,
            }],
        )
        .await?;

    let (stored_change_number, stored_change_key, stored_predecessor_list) =
        sqlx::query_as::<_, (i64, Vec<u8>, Vec<u8>)>(
            r#"
            SELECT mapi_change_number, change_key, predecessor_change_list
            FROM mapi_object_identities
            WHERE account_id = $1
              AND object_kind = 'calendar_event'
              AND canonical_id = $2
            "#,
        )
        .bind(account_id)
        .bind(event_id)
        .fetch_one(storage.pool())
        .await?;
    assert_eq!(stored_change_number, rotated_change_number as i64);
    assert_eq!(stored_change_key, rotated_change_key);
    assert_eq!(stored_predecessor_list, rotated_predecessor_list);

    // [MS-OXCFXICS] sections 2.2.1.2.7 and 3.1.5.3 allow an ICS import to
    // preserve a foreign ChangeKey even though the server assigns a distinct
    // internal CN. Identity repair must not replace that valid foreign XID.
    let mut imported_change_key = Uuid::parse_str("94949494-9494-4494-9494-949494949494")?
        .as_bytes()
        .to_vec();
    imported_change_key.extend_from_slice(&[0, 0, 0, 0, 0, 9]);
    let imported_predecessor_list = [
        vec![imported_change_key.len() as u8],
        imported_change_key.clone(),
    ]
    .concat();
    sqlx::query(
        r#"
        UPDATE mapi_object_identities
        SET change_key = $3,
            predecessor_change_list = $4
        WHERE account_id = $1
          AND object_kind = 'calendar_event'
          AND canonical_id = $2
        "#,
    )
    .bind(account_id)
    .bind(event_id)
    .bind(&imported_change_key)
    .bind(&imported_predecessor_list)
    .execute(storage.pool())
    .await?;

    storage
        .fetch_or_allocate_mapi_identities(
            account_id,
            &[MapiIdentityRequest {
                object_kind: MapiIdentityObjectKind::CalendarEvent,
                canonical_id: event_id,
                reserved_global_counter: None,
                source_key: None,
            }],
        )
        .await?;

    let (stored_change_key, stored_predecessor_list) = sqlx::query_as::<_, (Vec<u8>, Vec<u8>)>(
        r#"
            SELECT change_key, predecessor_change_list
            FROM mapi_object_identities
            WHERE account_id = $1
              AND object_kind = 'calendar_event'
              AND canonical_id = $2
            "#,
    )
    .bind(account_id)
    .bind(event_id)
    .fetch_one(storage.pool())
    .await?;
    assert_eq!(stored_change_key, imported_change_key);
    assert_eq!(stored_predecessor_list, imported_predecessor_list);

    // Reproduce the already-persisted Test 08:34 signature: the former helper
    // replaced CK71 with the object's creation CK70, while CN and PCL stayed
    // at 71. A reopen must repair only that proven stale local ChangeKey.
    let object_change_key = crate::mapi::identity::change_key_for_change_number(global_counter);
    sqlx::query(
        r#"
        UPDATE mapi_object_identities
        SET change_key = $3,
            predecessor_change_list = $4
        WHERE account_id = $1
          AND object_kind = 'calendar_event'
          AND canonical_id = $2
        "#,
    )
    .bind(account_id)
    .bind(event_id)
    .bind(&object_change_key)
    .bind(&rotated_predecessor_list)
    .execute(storage.pool())
    .await?;

    storage
        .fetch_or_allocate_mapi_identities(
            account_id,
            &[MapiIdentityRequest {
                object_kind: MapiIdentityObjectKind::CalendarEvent,
                canonical_id: event_id,
                reserved_global_counter: None,
                source_key: None,
            }],
        )
        .await?;

    let (stored_source_key, stored_change_key, stored_predecessor_list, stored_instance_key) =
        sqlx::query_as::<_, (Vec<u8>, Vec<u8>, Vec<u8>, Vec<u8>)>(
            r#"
            SELECT source_key, change_key, predecessor_change_list, instance_key
            FROM mapi_object_identities
            WHERE account_id = $1
              AND object_kind = 'calendar_event'
              AND canonical_id = $2
            "#,
        )
        .bind(account_id)
        .bind(event_id)
        .fetch_one(storage.pool())
        .await?;
    assert_eq!(stored_source_key, identity.source_key);
    assert_eq!(stored_instance_key, identity.source_key);
    assert_eq!(stored_change_key, rotated_change_key);
    assert_eq!(stored_predecessor_list, rotated_predecessor_list);

    fixture.cleanup().await?;
    Ok(())
}
