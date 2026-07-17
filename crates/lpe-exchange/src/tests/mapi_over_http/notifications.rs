use super::*;
use sqlx::Row;

fn notification_event_input(
    account_id: Uuid,
    event_id: Uuid,
    uid: &str,
    title: &str,
    sequence: i32,
) -> UpsertClientEventInput {
    UpsertClientEventInput {
        id: Some(event_id),
        account_id,
        uid: uid.to_string(),
        date: "2026-07-15".to_string(),
        time: "10:15".to_string(),
        time_zone: "Europe/Berlin".to_string(),
        duration_minutes: 45,
        all_day: false,
        status: "confirmed".to_string(),
        sequence,
        recurrence_rule: String::new(),
        recurrence_json: "{}".to_string(),
        recurrence_exceptions_json: "[]".to_string(),
        title: title.to_string(),
        location: "Outlook notification lab".to_string(),
        organizer_json: r#"{"email":"alice@example.test","common_name":"Alice Calendar"}"#
            .to_string(),
        attendees: "notification-grantee@example.test".to_string(),
        attendees_json: r#"{"attendees":[{"email":"notification-grantee@example.test","common_name":"Notification Grantee","role":"REQ-PARTICIPANT","partstat":"accepted","rsvp":false}]}"#.to_string(),
        notes: "Canonical Calendar notification regression".to_string(),
        body_html: "<p>Canonical Calendar notification regression</p>".to_string(),
    }
}

async fn insert_notification_account(
    storage: &Storage,
    owner_account_id: Uuid,
    account_id: Uuid,
    email: &str,
    display_name: &str,
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
    .bind(email)
    .bind(display_name)
    .bind(owner_account_id)
    .execute(storage.pool())
    .await?;
    Ok(())
}

async fn calendar_notification_ids(
    storage: &Storage,
    account_id: Uuid,
    collection_id: &str,
    event_id: Uuid,
) -> anyhow::Result<(u64, u64)> {
    let snapshot = storage.load_mapi_mail_store(account_id, 500).await?;
    let folder_id = snapshot
        .collaboration_folders()
        .iter()
        .find(|folder| folder.collection.id == collection_id)
        .map(|folder| folder.id)
        .ok_or_else(|| anyhow::anyhow!("custom Calendar folder was not projected"))?;
    let message_id = snapshot
        .events_for_folder(folder_id)
        .into_iter()
        .find(|event| event.canonical_id == event_id)
        .map(|event| event.id)
        .ok_or_else(|| anyhow::anyhow!("Calendar event was not projected"))?;
    Ok((folder_id, message_id))
}

fn assert_calendar_notification(
    poll: &MapiNotificationPoll,
    cursor: i64,
    event_mask: u16,
    folder_id: u64,
    message_id: u64,
    calendar_id: Uuid,
    event_id: Uuid,
) {
    assert!(poll.event_pending);
    assert_eq!(poll.cursor, Some(cursor));
    assert_eq!(poll.events.len(), 1);
    let event = &poll.events[0];
    assert_eq!(
        event.notification_test_shape(),
        (
            MapiNotificationKind::Content,
            event_mask,
            folder_id,
            Some(message_id),
            None,
            None,
            Some("calendar_event"),
        )
    );
    assert_eq!(event.canonical_folder_id(), Some(calendar_id));
    assert_eq!(event.canonical_message_id(), Some(event_id));
}

async fn assert_outsider_has_no_notifications(
    storage: &Storage,
    outsider_account_id: Uuid,
    after_cursor: i64,
) -> anyhow::Result<()> {
    let poll = storage
        .poll_mapi_notifications(outsider_account_id, after_cursor)
        .await?;
    assert!(!poll.event_pending);
    assert!(poll.events.is_empty());
    Ok(())
}

#[tokio::test]
async fn mapi_calendar_move_notifications_are_replayed_with_old_and_new_ids_from_postgresql(
) -> anyhow::Result<()> {
    let Some(fixture) = postgres_mapi_calendar_fixture().await? else {
        return Ok(());
    };
    let storage = fixture.storage.clone();
    let account_id = fixture.account_id;
    let collection = storage
        .create_accessible_calendar_collection(account_id, "Move notification lab")
        .await?;
    let event_id = Uuid::parse_str("82828282-8282-4282-9282-828282828282")?;
    storage
        .create_accessible_event(
            account_id,
            Some(&collection.id),
            notification_event_input(
                account_id,
                event_id,
                "mapi-calendar-move-notification-postgresql",
                "Calendar move notification",
                0,
            ),
        )
        .await?;
    let (source_folder_id, old_message_id) =
        calendar_notification_ids(&storage, account_id, &collection.id, event_id).await?;
    let trash_mailbox_id = storage
        .ensure_jmap_system_mailboxes(account_id)
        .await?
        .into_iter()
        .find(|mailbox| mailbox.role == "trash")
        .map(|mailbox| mailbox.id)
        .expect("canonical Deleted Items mailbox");
    let trash_checkpoint = storage
        .fetch_mapi_sync_changes(
            account_id,
            Some(trash_mailbox_id),
            MapiCheckpointKind::Content,
            0,
        )
        .await?
        .current_change_sequence;
    let baseline_cursor = storage
        .fetch_mapi_notification_cursor(account_id)
        .await?
        .unwrap_or(0);

    let moved = storage
        .move_accessible_event_to_deleted_items(account_id, event_id, None)
        .await?;
    let identity = moved.principal_identity.expect("owner Event move identity");
    assert_eq!(identity.old_mapi_object_id, old_message_id);
    assert_eq!(
        storage
            .fetch_mapi_object_ids_for_deleted_changes(
                account_id,
                MapiIdentityObjectKind::CalendarEvent,
                &[event_id],
            )
            .await?,
        vec![old_message_id]
    );
    let trash_changes = storage
        .fetch_mapi_sync_changes(
            account_id,
            Some(trash_mailbox_id),
            MapiCheckpointKind::Content,
            trash_checkpoint,
        )
        .await?;
    assert!(trash_changes
        .changed_deleted_calendar_event_ids
        .contains(&event_id));

    // A second MAPI session starts from the pre-move cursor. The durable poll
    // must reconstruct both [MS-OXCNOTIF] source deletion and destination move
    // fields from the canonical logs plus the persisted identity-move record.
    let poll = storage
        .poll_mapi_notifications(account_id, baseline_cursor)
        .await?;
    assert!(poll.event_pending);
    assert_eq!(poll.events.len(), 2);
    assert_eq!(
        poll.events[0].notification_test_shape(),
        (
            MapiNotificationKind::Content,
            0x0008,
            source_folder_id,
            Some(old_message_id),
            None,
            None,
            Some("calendar_event"),
        )
    );
    assert_eq!(
        poll.events[1].notification_test_shape(),
        (
            MapiNotificationKind::Content,
            0x0020,
            crate::mapi::identity::TRASH_FOLDER_ID,
            Some(identity.new_mapi_object_id),
            Some(source_folder_id),
            Some(old_message_id),
            Some("deleted_calendar_event"),
        )
    );

    fixture.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn mapi_calendar_notifications_are_durable_and_principal_scoped_in_postgresql(
) -> anyhow::Result<()> {
    let Some(fixture) = postgres_mapi_calendar_fixture().await? else {
        return Ok(());
    };
    let storage = fixture.storage.clone();
    let owner_account_id = fixture.account_id;
    let grantee_account_id = Uuid::parse_str("10000000-0000-0000-0000-000000000010")?;
    let outsider_account_id = Uuid::parse_str("10000000-0000-0000-0000-000000000011")?;
    insert_notification_account(
        &storage,
        owner_account_id,
        grantee_account_id,
        "notification-grantee@example.test",
        "Notification Grantee",
    )
    .await?;
    insert_notification_account(
        &storage,
        owner_account_id,
        outsider_account_id,
        "notification-outsider@example.test",
        "Notification Outsider",
    )
    .await?;

    let collection = storage
        .create_accessible_calendar_collection(owner_account_id, "Shared Outlook Lab")
        .await?;
    let calendar_id = Uuid::parse_str(&collection.id)?;
    storage
        .upsert_collaboration_grant(
            CollaborationGrantInput {
                kind: CollaborationResourceKind::Calendar,
                owner_account_id,
                grantee_email: "notification-grantee@example.test".to_string(),
                calendar_id: Some(calendar_id),
                may_read: true,
                may_write: false,
                may_delete: false,
                may_share: false,
            },
            lpe_storage::AuditEntryInput {
                actor: "alice@example.test".to_string(),
                action: "test-mapi-calendar-notification-grant".to_string(),
                subject: calendar_id.to_string(),
            },
        )
        .await?;

    let event_id = Uuid::parse_str("81818181-8181-4181-9181-818181818181")?;
    let event_uid = "mapi-calendar-notification-postgresql";
    storage
        .create_accessible_event(
            owner_account_id,
            Some(&collection.id),
            notification_event_input(
                owner_account_id,
                event_id,
                event_uid,
                "Calendar notification created",
                0,
            ),
        )
        .await?;

    let owner_ids =
        calendar_notification_ids(&storage, owner_account_id, &collection.id, event_id).await?;
    storage
        .fetch_or_allocate_mapi_identities(
            grantee_account_id,
            &[MapiIdentityRequest {
                object_kind: MapiIdentityObjectKind::Account,
                canonical_id: owner_account_id,
                reserved_global_counter: None,
                source_key: None,
            }],
        )
        .await?;
    let grantee_ids =
        calendar_notification_ids(&storage, grantee_account_id, &collection.id, event_id).await?;
    assert_ne!(owner_ids.0, grantee_ids.0);
    assert_ne!(owner_ids.1, grantee_ids.1);

    storage
        .fetch_or_allocate_mapi_identities(
            outsider_account_id,
            &[MapiIdentityRequest {
                object_kind: MapiIdentityObjectKind::CalendarEvent,
                canonical_id: event_id,
                reserved_global_counter: None,
                source_key: None,
            }],
        )
        .await?;

    let baseline_cursor = storage
        .fetch_mapi_notification_cursor(owner_account_id)
        .await?
        .unwrap_or(0);
    let event_modseq =
        sqlx::query_scalar::<_, i64>("SELECT modseq FROM calendar_events WHERE id = $1")
            .bind(event_id)
            .fetch_one(storage.pool())
            .await?;
    let affected_principals = vec![owner_account_id, grantee_account_id];
    let created_cursor = sqlx::query_scalar::<_, i64>(
        r#"
        INSERT INTO mail_change_log (
            tenant_id, account_id, collection_id, object_kind, object_id,
            change_kind, modseq, affected_principal_ids, summary_json
        )
        SELECT
            tenant_id, $1, $2, 'calendar_event', $3,
            'created', $4, $5, $6
        FROM accounts
        WHERE id = $1
        RETURNING cursor
        "#,
    )
    .bind(owner_account_id)
    .bind(calendar_id)
    .bind(event_id)
    .bind(event_modseq)
    .bind(&affected_principals)
    .bind(serde_json::json!({
        "collectionId": calendar_id,
        "objectUid": event_uid,
    }))
    .fetch_one(storage.pool())
    .await?;
    assert!(created_cursor > baseline_cursor);

    let owner_created = storage
        .poll_mapi_notifications(owner_account_id, baseline_cursor)
        .await?;
    assert_calendar_notification(
        &owner_created,
        created_cursor,
        0x0004,
        owner_ids.0,
        owner_ids.1,
        calendar_id,
        event_id,
    );
    let grantee_created = storage
        .poll_mapi_notifications(grantee_account_id, baseline_cursor)
        .await?;
    assert_calendar_notification(
        &grantee_created,
        created_cursor,
        0x0004,
        grantee_ids.0,
        grantee_ids.1,
        calendar_id,
        event_id,
    );
    assert_outsider_has_no_notifications(&storage, outsider_account_id, baseline_cursor).await?;

    storage
        .update_accessible_event(
            owner_account_id,
            event_id,
            notification_event_input(
                owner_account_id,
                event_id,
                event_uid,
                "Calendar notification updated",
                1,
            ),
        )
        .await?;
    let updated_cursor = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT MAX(cursor)
        FROM mail_change_log
        WHERE object_kind = 'calendar_event'
          AND object_id = $1
          AND change_kind = 'updated'
          AND cursor > $2
        "#,
    )
    .bind(event_id)
    .bind(created_cursor)
    .fetch_one(storage.pool())
    .await?;
    let owner_updated = storage
        .poll_mapi_notifications(owner_account_id, created_cursor)
        .await?;
    assert_calendar_notification(
        &owner_updated,
        updated_cursor,
        0x0010,
        owner_ids.0,
        owner_ids.1,
        calendar_id,
        event_id,
    );
    let grantee_updated = storage
        .poll_mapi_notifications(grantee_account_id, created_cursor)
        .await?;
    assert_calendar_notification(
        &grantee_updated,
        updated_cursor,
        0x0010,
        grantee_ids.0,
        grantee_ids.1,
        calendar_id,
        event_id,
    );
    assert_outsider_has_no_notifications(&storage, outsider_account_id, created_cursor).await?;

    storage
        .delete_accessible_event(owner_account_id, event_id)
        .await?;
    let deleted_row = sqlx::query(
        r#"
        SELECT
            log.cursor,
            log.collection_id,
            log.affected_principal_ids,
            log.summary_json,
            tombstone.collection_id AS tombstone_collection_id,
            tombstone.object_uid
        FROM mail_change_log log
        JOIN tombstones tombstone
          ON tombstone.tenant_id = log.tenant_id
         AND tombstone.change_cursor = log.cursor
         AND tombstone.object_kind = log.object_kind
         AND tombstone.object_id = log.object_id
        WHERE log.object_kind = 'calendar_event'
          AND log.object_id = $1
          AND log.change_kind = 'destroyed'
          AND log.cursor > $2
        "#,
    )
    .bind(event_id)
    .bind(updated_cursor)
    .fetch_one(storage.pool())
    .await?;
    let deleted_cursor = deleted_row.get::<i64, _>("cursor");
    assert_eq!(deleted_row.get::<Uuid, _>("collection_id"), calendar_id);
    assert_eq!(
        deleted_row.get::<Uuid, _>("tombstone_collection_id"),
        calendar_id
    );
    assert_eq!(deleted_row.get::<String, _>("object_uid"), event_uid);
    assert_eq!(
        deleted_row
            .get::<serde_json::Value, _>("summary_json")
            .get("collectionId")
            .and_then(serde_json::Value::as_str),
        Some(collection.id.as_str())
    );
    assert_eq!(
        deleted_row.get::<Vec<Uuid>, _>("affected_principal_ids"),
        affected_principals
    );
    let destination_cursor = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT cursor
        FROM mail_change_log
        WHERE object_kind = 'deleted_calendar_event'
          AND object_id = $1
          AND change_kind = 'created'
          AND cursor > $2
        "#,
    )
    .bind(event_id)
    .bind(deleted_cursor)
    .fetch_one(storage.pool())
    .await?;
    let identity_moves = sqlx::query(
        r#"
        SELECT account_id, old_mapi_object_id, new_mapi_object_id
        FROM mapi_calendar_event_identity_moves
        WHERE event_id = $1
        "#,
    )
    .bind(event_id)
    .fetch_all(storage.pool())
    .await?
    .into_iter()
    .map(|row| {
        (
            row.get::<Uuid, _>("account_id"),
            (
                row.get::<i64, _>("old_mapi_object_id") as u64,
                row.get::<i64, _>("new_mapi_object_id") as u64,
            ),
        )
    })
    .collect::<HashMap<_, _>>();

    let owner_deleted = storage
        .poll_mapi_notifications(owner_account_id, updated_cursor)
        .await?;
    assert_eq!(owner_deleted.cursor, Some(destination_cursor));
    assert_eq!(owner_deleted.events.len(), 2);
    assert_eq!(
        owner_deleted.events[0].notification_test_shape(),
        (
            MapiNotificationKind::Content,
            0x0008,
            owner_ids.0,
            Some(owner_ids.1),
            None,
            None,
            Some("calendar_event"),
        )
    );
    assert_eq!(
        owner_deleted.events[1].notification_test_shape(),
        (
            MapiNotificationKind::Content,
            0x0020,
            crate::mapi::identity::TRASH_FOLDER_ID,
            Some(identity_moves[&owner_account_id].1),
            Some(owner_ids.0),
            Some(owner_ids.1),
            Some("deleted_calendar_event"),
        )
    );
    let grantee_deleted = storage
        .poll_mapi_notifications(grantee_account_id, updated_cursor)
        .await?;
    assert_eq!(grantee_deleted.cursor, Some(destination_cursor));
    assert_eq!(grantee_deleted.events.len(), 2);
    assert_eq!(
        grantee_deleted.events[0].notification_test_shape(),
        (
            MapiNotificationKind::Content,
            0x0008,
            grantee_ids.0,
            Some(grantee_ids.1),
            None,
            None,
            Some("calendar_event"),
        )
    );
    assert_eq!(
        grantee_deleted.events[1].notification_test_shape(),
        (
            MapiNotificationKind::Content,
            0x0020,
            crate::mapi::identity::TRASH_FOLDER_ID,
            Some(identity_moves[&grantee_account_id].1),
            Some(grantee_ids.0),
            Some(grantee_ids.1),
            Some("deleted_calendar_event"),
        )
    );
    assert_outsider_has_no_notifications(&storage, outsider_account_id, updated_cursor).await?;

    assert_eq!(identity_moves[&owner_account_id].0, owner_ids.1);
    assert_eq!(identity_moves[&grantee_account_id].0, grantee_ids.1);
    assert_ne!(
        identity_moves[&owner_account_id].0,
        identity_moves[&owner_account_id].1
    );
    assert_ne!(
        identity_moves[&grantee_account_id].0,
        identity_moves[&grantee_account_id].1
    );
    let destination_identities = sqlx::query(
        r#"
        SELECT account_id, mapi_object_id, deleted_at IS NOT NULL AS retired
        FROM mapi_object_identities
        WHERE object_kind = 'deleted_calendar_event'
          AND canonical_id = $1
        "#,
    )
    .bind(event_id)
    .fetch_all(storage.pool())
    .await?;
    let destination_identities = destination_identities
        .into_iter()
        .map(|row| {
            (
                row.get::<Uuid, _>("account_id"),
                (
                    row.get::<i64, _>("mapi_object_id") as u64,
                    row.get::<bool, _>("retired"),
                ),
            )
        })
        .collect::<HashMap<_, _>>();
    assert_eq!(
        destination_identities[&owner_account_id],
        (identity_moves[&owner_account_id].1, false)
    );
    assert_eq!(
        destination_identities[&grantee_account_id],
        (identity_moves[&grantee_account_id].1, false)
    );
    assert_eq!(
        destination_identities[&outsider_account_id],
        (identity_moves[&outsider_account_id].1, false)
    );

    let change_kinds = sqlx::query_scalar::<_, String>(
        r#"
        SELECT change_kind
        FROM mail_change_log
        WHERE object_kind = 'calendar_event'
          AND object_id = $1
          AND cursor > $2
        ORDER BY cursor
        "#,
    )
    .bind(event_id)
    .bind(baseline_cursor)
    .fetch_all(storage.pool())
    .await?;
    assert_eq!(change_kinds, ["created", "updated", "destroyed"]);

    // [MS-OXCNOTIF] sections 2.2.1.1 and 2.2.1.4.1.2 require the
    // ObjectCreated/ObjectModified/ObjectDeleted/ObjectMoved message
    // notifications above to retain each principal's exact old/new IDs.
    fixture.cleanup().await?;
    Ok(())
}
