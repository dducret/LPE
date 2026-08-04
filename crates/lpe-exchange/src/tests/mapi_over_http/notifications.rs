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

fn assert_navigation_shortcut_notification(
    poll: &MapiNotificationPoll,
    cursor: i64,
    event_mask: u16,
    message_id: u64,
    shortcut_id: Uuid,
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
            crate::mapi::identity::COMMON_VIEWS_FOLDER_ID,
            Some(message_id),
            None,
            None,
            Some("navigation_shortcut"),
        )
    );
    assert_eq!(event.canonical_folder_id(), None);
    assert_eq!(event.canonical_message_id(), Some(shortcut_id));
}

async fn navigation_shortcut_notification_cursor(
    storage: &Storage,
    account_id: Uuid,
    shortcut_id: Uuid,
    change_kind: &str,
    after_cursor: i64,
) -> anyhow::Result<i64> {
    Ok(sqlx::query_scalar::<_, i64>(
        r#"
        SELECT cursor
        FROM mail_change_log
        WHERE account_id = $1
          AND object_kind = 'navigation_shortcut'
          AND object_id = $2
          AND change_kind = $3
          AND cursor > $4
        ORDER BY cursor DESC
        LIMIT 1
        "#,
    )
    .bind(account_id)
    .bind(shortcut_id)
    .bind(change_kind)
    .bind(after_cursor)
    .fetch_one(storage.pool())
    .await?)
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
async fn mapi_inbox_new_mail_notification_allocates_recipient_scoped_message_identity_in_postgresql(
) -> anyhow::Result<()> {
    // [MS-OXCNOTIF] sections 2.2.1.1 and 4 require NewMail to carry the
    // recipient's receive-folder ID and durable Message ID.
    let Some(fixture) = postgres_mapi_calendar_fixture().await? else {
        return Ok(());
    };
    let storage = fixture.storage.clone();
    let owner_account_id = fixture.account_id;
    let account_id = Uuid::parse_str("10000000-0000-0000-0000-000000000012")?;
    insert_notification_account(
        &storage,
        owner_account_id,
        account_id,
        "notification-recipient@example.test",
        "Notification Recipient",
    )
    .await?;
    let inbox_id = storage
        .ensure_jmap_system_mailboxes(owner_account_id)
        .await?
        .into_iter()
        .find(|mailbox| mailbox.role == "inbox")
        .map(|mailbox| mailbox.id)
        .expect("canonical Inbox mailbox");
    let baseline_cursor = storage
        .fetch_mapi_notification_cursor(account_id)
        .await?
        .unwrap_or(0);
    let imported = storage
        .import_jmap_email(
            JmapImportedEmailInput {
                account_id: owner_account_id,
                submitted_by_account_id: owner_account_id,
                mailbox_id: inbox_id,
                source: "inbound-smtp".to_string(),
                raw_message: None,
                from_display: Some("Reply Sender".to_string()),
                from_address: "reply@example.test".to_string(),
                sender_display: None,
                sender_address: None,
                to: Vec::new(),
                cc: Vec::new(),
                bcc: Vec::new(),
                subject: "Reply visible through NewMail".to_string(),
                body_text: "Inbound reply identity regression".to_string(),
                body_html_sanitized: None,
                internet_message_id: Some(format!("<{}@example.test>", Uuid::new_v4())),
                mime_blob_ref: String::new(),
                size_octets: 64,
                received_at: None,
                thread_id: None,
                attachments: Vec::new(),
            },
            postgres_mapi_audit("import-inbound-reply", owner_account_id),
        )
        .await?;
    // import_jmap_email is a generic import path and marks the membership as
    // read. Inbound SMTP delivery creates an unread Inbox membership.
    sqlx::query(
        r#"
        UPDATE mailbox_messages
        SET is_seen = FALSE
        WHERE account_id = $1
          AND mailbox_id = $2
          AND message_id = $3
        "#,
    )
    .bind(owner_account_id)
    .bind(inbox_id)
    .bind(imported.id)
    .execute(storage.pool())
    .await?;
    let created_cursor = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT cursor
        FROM mail_change_log
        WHERE account_id = $1
          AND mailbox_id = $2
          AND object_kind = 'mailbox_message'
          AND change_kind = 'created'
          AND summary_json->>'messageId' = $3
          AND cursor > $4
        ORDER BY cursor DESC
        LIMIT 1
        "#,
    )
    .bind(owner_account_id)
    .bind(inbox_id)
    .bind(imported.id.to_string())
    .bind(baseline_cursor)
    .fetch_one(storage.pool())
    .await?;
    // mail_change_log is append-only. Record the recipient's delivery scope
    // as the access layer would, without rewriting the owner's original row.
    let recipient_created_cursor = sqlx::query_scalar::<_, i64>(
        r#"
        INSERT INTO mail_change_log (
            tenant_id, account_id, mailbox_id, object_kind, object_id, change_kind,
            modseq, affected_principal_ids, summary_json
        )
        SELECT
            tenant_id, account_id, mailbox_id, object_kind, object_id, change_kind,
            modseq, ARRAY[account_id, $1]::uuid[], summary_json
        FROM mail_change_log
        WHERE cursor = $2
        RETURNING cursor
        "#,
    )
    .bind(account_id)
    .bind(created_cursor)
    .fetch_one(storage.pool())
    .await?;
    let owner_message_id = storage
        .fetch_or_allocate_mapi_identities(
            owner_account_id,
            &[MapiIdentityRequest {
                object_kind: MapiIdentityObjectKind::Message,
                canonical_id: imported.id,
                reserved_global_counter: None,
                source_key: None,
            }],
        )
        .await?
        .remove(0)
        .object_id;
    let identity_before_poll = sqlx::query_scalar::<_, Option<i64>>(
        r#"
        SELECT mapi_object_id
        FROM mapi_object_identities
        WHERE account_id = $1
          AND object_kind = 'message'
          AND canonical_id = $2
          AND deleted_at IS NULL
        "#,
    )
    .bind(account_id)
    .bind(imported.id)
    .fetch_optional(storage.pool())
    .await?
    .flatten();
    assert!(identity_before_poll.is_none());

    let poll = storage
        .poll_mapi_notifications(account_id, baseline_cursor)
        .await?;
    let message_id = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT mapi_object_id
        FROM mapi_object_identities
        WHERE account_id = $1
          AND object_kind = 'message'
          AND canonical_id = $2
          AND deleted_at IS NULL
        "#,
    )
    .bind(account_id)
    .bind(imported.id)
    .fetch_one(storage.pool())
    .await? as u64;

    assert_ne!(message_id, owner_message_id);

    assert!(poll.event_pending);
    assert_eq!(poll.cursor, Some(recipient_created_cursor));
    assert_eq!(poll.events.len(), 1);
    assert_eq!(
        poll.events[0].notification_test_shape(),
        (
            MapiNotificationKind::Content,
            0x0002,
            crate::mapi::identity::INBOX_FOLDER_ID,
            Some(message_id),
            None,
            None,
            Some("mailbox_message"),
        )
    );
    assert_eq!(poll.events[0].canonical_folder_id(), Some(inbox_id));
    assert_eq!(poll.events[0].canonical_message_id(), Some(imported.id));
    // [MS-OXCNOTIF] section 2.2.1.4.1.2, implementation note <10>:
    // Exchange 2016 test1_202608031300.saz raw/753 sends zero for this
    // notification field. It is not the canonical PidTagMessageFlags value.
    assert_eq!(poll.events[0].new_mail_message_flags(), None);

    fixture.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn mapi_non_inbox_message_notification_allocates_message_identity_in_postgresql(
) -> anyhow::Result<()> {
    // [MS-OXCNOTIF] section 2.2.1.4.1.2 requires ObjectCreated to carry both
    // FolderId and MessageId when it represents a message, including Sent.
    let Some(fixture) = postgres_mapi_calendar_fixture().await? else {
        return Ok(());
    };
    let storage = fixture.storage.clone();
    let account_id = fixture.account_id;
    let sent_id = storage
        .ensure_jmap_system_mailboxes(account_id)
        .await?
        .into_iter()
        .find(|mailbox| mailbox.role == "sent")
        .map(|mailbox| mailbox.id)
        .expect("canonical Sent mailbox");
    let baseline_cursor = storage
        .fetch_mapi_notification_cursor(account_id)
        .await?
        .unwrap_or(0);
    let imported = storage
        .import_jmap_email(
            JmapImportedEmailInput {
                account_id,
                submitted_by_account_id: account_id,
                mailbox_id: sent_id,
                source: "mapi-notification-regression".to_string(),
                raw_message: None,
                from_display: Some("Sent Sender".to_string()),
                from_address: "sender@example.test".to_string(),
                sender_display: None,
                sender_address: None,
                to: Vec::new(),
                cc: Vec::new(),
                bcc: Vec::new(),
                subject: "Sent message identity notification".to_string(),
                body_text: "Sent message notification identity regression".to_string(),
                body_html_sanitized: None,
                internet_message_id: Some(format!("<{}@example.test>", Uuid::new_v4())),
                mime_blob_ref: String::new(),
                size_octets: 64,
                received_at: None,
                thread_id: None,
                attachments: Vec::new(),
            },
            postgres_mapi_audit("import-sent-message", account_id),
        )
        .await?;
    let created_cursor = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT cursor
        FROM mail_change_log
        WHERE account_id = $1
          AND mailbox_id = $2
          AND object_kind = 'mailbox_message'
          AND change_kind = 'created'
          AND summary_json->>'messageId' = $3
          AND cursor > $4
        ORDER BY cursor DESC
        LIMIT 1
        "#,
    )
    .bind(account_id)
    .bind(sent_id)
    .bind(imported.id.to_string())
    .bind(baseline_cursor)
    .fetch_one(storage.pool())
    .await?;
    let identity_before_poll = sqlx::query_scalar::<_, Option<i64>>(
        r#"
        SELECT mapi_object_id
        FROM mapi_object_identities
        WHERE account_id = $1
          AND object_kind = 'message'
          AND canonical_id = $2
          AND deleted_at IS NULL
        "#,
    )
    .bind(account_id)
    .bind(imported.id)
    .fetch_optional(storage.pool())
    .await?
    .flatten();
    assert!(identity_before_poll.is_none());

    let poll = storage
        .poll_mapi_notifications(account_id, baseline_cursor)
        .await?;
    let message_id = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT mapi_object_id
        FROM mapi_object_identities
        WHERE account_id = $1
          AND object_kind = 'message'
          AND canonical_id = $2
          AND deleted_at IS NULL
        "#,
    )
    .bind(account_id)
    .bind(imported.id)
    .fetch_one(storage.pool())
    .await? as u64;

    assert!(poll.event_pending);
    assert_eq!(poll.cursor, Some(created_cursor));
    assert_eq!(poll.events.len(), 1);
    assert_eq!(
        poll.events[0].notification_test_shape(),
        (
            MapiNotificationKind::Content,
            0x0004,
            crate::mapi::identity::SENT_FOLDER_ID,
            Some(message_id),
            None,
            None,
            Some("mailbox_message"),
        )
    );
    assert_eq!(poll.events[0].canonical_folder_id(), Some(sent_id));
    assert_eq!(poll.events[0].canonical_message_id(), Some(imported.id));

    fixture.cleanup().await?;
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

#[tokio::test]
async fn mapi_navigation_shortcut_notifications_are_durable_across_storage_instances_in_postgresql(
) -> anyhow::Result<()> {
    let Some(fixture) = postgres_mapi_calendar_fixture().await? else {
        return Ok(());
    };
    let writer = fixture.storage.clone();
    let reader = fixture.storage.clone();
    let account_id = fixture.account_id;
    let shortcut_id = Uuid::parse_str("c414c414-c414-4414-8414-c414c414c414")?;
    let input = |subject: &str, ordinal: u8| crate::store::UpsertMapiNavigationShortcutInput {
        id: Some(shortcut_id),
        account_id,
        subject: subject.to_string(),
        target_folder_id: Some(crate::mapi::identity::CONTACTS_FOLDER_ID),
        shortcut_type: 0,
        flags: 0,
        save_stamp: 1_537_819_608,
        section: 4,
        ordinal: vec![ordinal],
        group_header_id: Some(Uuid::parse_str("b7f00600-0000-0000-c000-000000000046").unwrap()),
        group_name: "My Contacts".to_string(),
        client_properties: crate::store::MapiNavigationShortcutClientProperties::default(),
    };
    let baseline_cursor = reader
        .fetch_mapi_notification_cursor(account_id)
        .await?
        .unwrap_or(0);

    let identity = writer
        .commit_mapi_navigation_shortcut_create(
            crate::store::CommitMapiNavigationShortcutCreateInput {
                shortcut: input("Contacts", 127),
            },
        )
        .await?
        .identity;
    let created_cursor = navigation_shortcut_notification_cursor(
        &writer,
        account_id,
        shortcut_id,
        "created",
        baseline_cursor,
    )
    .await?;
    let created = reader
        .poll_mapi_notifications(account_id, baseline_cursor)
        .await?;
    assert_navigation_shortcut_notification(
        &created,
        created_cursor,
        0x0004,
        identity.object_id,
        shortcut_id,
    );

    writer
        .upsert_mapi_navigation_shortcut(input("Contacts renamed", 191))
        .await?;
    let updated_cursor = navigation_shortcut_notification_cursor(
        &writer,
        account_id,
        shortcut_id,
        "updated",
        created_cursor,
    )
    .await?;
    let updated = reader
        .poll_mapi_notifications(account_id, created_cursor)
        .await?;
    assert_navigation_shortcut_notification(
        &updated,
        updated_cursor,
        0x0010,
        identity.object_id,
        shortcut_id,
    );

    writer
        .delete_mapi_navigation_shortcut(account_id, shortcut_id)
        .await?;
    let deleted_cursor = navigation_shortcut_notification_cursor(
        &writer,
        account_id,
        shortcut_id,
        "destroyed",
        updated_cursor,
    )
    .await?;
    let deleted = reader
        .poll_mapi_notifications(account_id, updated_cursor)
        .await?;
    assert_navigation_shortcut_notification(
        &deleted,
        deleted_cursor,
        0x0008,
        identity.object_id,
        shortcut_id,
    );

    // [MS-OXOCFG] sections 2.2.9 and 3.1.4.9 make WLinks Common Views
    // FAI rows. [MS-OXCNOTIF] sections 2.2.1.1, 2.2.1.1.1, 3.1.4.3,
    // and 3.2.4.2 require another client viewing that table to observe its
    // create, update, and delete changes through table notifications.
    fixture.cleanup().await?;
    Ok(())
}

async fn create_notification_mailbox(
    storage: &Storage,
    account_id: Uuid,
    name: &str,
    parent_id: Option<Uuid>,
) -> anyhow::Result<JmapMailbox> {
    Ok(storage
        .create_jmap_mailbox(
            JmapMailboxCreateInput {
                account_id,
                name: name.to_string(),
                parent_id,
                sort_order: None,
                is_subscribed: true,
                copy_source_mailbox_id: None,
            },
            postgres_mapi_audit("create-notification-mailbox", account_id),
        )
        .await?)
}

#[tokio::test]
async fn mapi_hierarchy_move_and_copy_replay_is_recipient_scoped_and_historical_in_postgresql(
) -> anyhow::Result<()> {
    let Some(fixture) = postgres_mapi_calendar_fixture().await? else {
        return Ok(());
    };
    let storage = fixture.storage.clone();
    let owner_account_id = fixture.account_id;
    let grantee_account_id = Uuid::parse_str("10000000-0000-0000-0000-000000000021")?;
    insert_notification_account(
        &storage,
        owner_account_id,
        grantee_account_id,
        "folder-replay-grantee@example.test",
        "Folder Replay Grantee",
    )
    .await?;
    storage
        .ensure_jmap_system_mailboxes(owner_account_id)
        .await?;
    storage
        .upsert_mailbox_delegation_grant(
            lpe_storage::MailboxDelegationGrantInput {
                owner_account_id,
                grantee_email: "folder-replay-grantee@example.test".to_string(),
                may_write: false,
            },
            postgres_mapi_audit("grant-folder-replay", grantee_account_id),
        )
        .await?;

    let source_parent =
        create_notification_mailbox(&storage, owner_account_id, "Projects", None).await?;
    let middle_parent =
        create_notification_mailbox(&storage, owner_account_id, "Clients", None).await?;
    let destination_parent =
        create_notification_mailbox(&storage, owner_account_id, "Archive", None).await?;
    let moving_folder = create_notification_mailbox(
        &storage,
        owner_account_id,
        "Quarterly reports",
        Some(source_parent.id),
    )
    .await?;
    let canonical_ids = vec![
        source_parent.id,
        middle_parent.id,
        destination_parent.id,
        moving_folder.id,
    ];
    let owner_identities = storage
        .fetch_or_allocate_mapi_identities(
            owner_account_id,
            &canonical_ids
                .iter()
                .copied()
                .map(|canonical_id| MapiIdentityRequest {
                    object_kind: MapiIdentityObjectKind::Mailbox,
                    canonical_id,
                    reserved_global_counter: None,
                    source_key: None,
                })
                .collect::<Vec<_>>(),
        )
        .await?;
    let owner_folder_ids = owner_identities
        .into_iter()
        .map(|identity| (identity.canonical_id, identity.object_id))
        .collect::<HashMap<_, _>>();
    let grantee_identity_count = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*)
        FROM mapi_object_identities
        WHERE account_id = $1
          AND object_kind = 'mailbox'
          AND canonical_id = ANY($2)
        "#,
    )
    .bind(grantee_account_id)
    .bind(&canonical_ids)
    .fetch_one(storage.pool())
    .await?;
    assert_eq!(grantee_identity_count, 0);

    let baseline_cursor = storage
        .fetch_mapi_notification_cursor(owner_account_id)
        .await?
        .unwrap_or(0);
    for parent_id in [middle_parent.id, destination_parent.id] {
        storage
            .update_jmap_mailbox(
                JmapMailboxUpdateInput {
                    account_id: owner_account_id,
                    mailbox_id: moving_folder.id,
                    name: None,
                    parent_id: Some(Some(parent_id)),
                    sort_order: None,
                    is_subscribed: None,
                },
                postgres_mapi_audit("move-notification-mailbox", moving_folder.id),
            )
            .await?;
    }

    let moved = storage
        .poll_mapi_notifications(grantee_account_id, baseline_cursor)
        .await?;
    assert!(moved.event_pending);
    assert_eq!(moved.events.len(), 4);
    let source_parent_id = moved.events[1].notification_test_shape().2;
    let middle_parent_id = moved.events[0].notification_test_shape().2;
    let destination_parent_id = moved.events[2].notification_test_shape().2;
    let moving_folder_id = moved.events[0]
        .notification_test_shape()
        .3
        .expect("moved folder ID");
    assert_ne!(moving_folder_id, owner_folder_ids[&moving_folder.id]);
    assert_ne!(source_parent_id, owner_folder_ids[&source_parent.id]);
    assert_ne!(middle_parent_id, owner_folder_ids[&middle_parent.id]);
    assert_ne!(
        destination_parent_id,
        owner_folder_ids[&destination_parent.id]
    );
    assert_eq!(
        moved.events[0].notification_test_shape(),
        (
            MapiNotificationKind::Hierarchy,
            0x0020,
            middle_parent_id,
            Some(moving_folder_id),
            Some(moving_folder_id),
            None,
            Some("mailbox"),
        )
    );
    assert_eq!(
        moved.events[0].old_parent_folder_id(),
        Some(source_parent_id)
    );
    assert!(moved.events[0].is_complete_for_wire());
    assert_eq!(
        moved.events[1].notification_test_shape(),
        (
            MapiNotificationKind::Hierarchy,
            0x0100,
            source_parent_id,
            Some(moving_folder_id),
            None,
            None,
            None,
        )
    );
    assert_eq!(
        moved.events[2].notification_test_shape(),
        (
            MapiNotificationKind::Hierarchy,
            0x0020,
            destination_parent_id,
            Some(moving_folder_id),
            Some(moving_folder_id),
            None,
            Some("mailbox"),
        )
    );
    assert_eq!(
        moved.events[2].old_parent_folder_id(),
        Some(middle_parent_id)
    );
    assert!(moved.events[2].is_complete_for_wire());
    assert_eq!(
        moved.events[3].notification_test_shape(),
        (
            MapiNotificationKind::Hierarchy,
            0x0100,
            middle_parent_id,
            Some(moving_folder_id),
            None,
            None,
            None,
        )
    );
    let moved_grantee_identity_count = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*)
        FROM mapi_object_identities
        WHERE account_id = $1
          AND object_kind = 'mailbox'
          AND canonical_id = ANY($2)
          AND deleted_at IS NULL
        "#,
    )
    .bind(grantee_account_id)
    .bind(vec![source_parent.id, middle_parent.id, moving_folder.id])
    .fetch_one(storage.pool())
    .await?;
    assert_eq!(moved_grantee_identity_count, 3);
    storage
        .store_mapi_sync_checkpoint(
            grantee_account_id,
            Some(moving_folder.id),
            MapiCheckpointKind::Content,
            7,
            11,
            serde_json::json!({"sharedMailbox": true}),
        )
        .await?;

    let copied_folder = storage
        .create_jmap_mailbox(
            JmapMailboxCreateInput {
                account_id: owner_account_id,
                name: "Quarterly reports copy".to_string(),
                parent_id: Some(destination_parent.id),
                sort_order: None,
                is_subscribed: true,
                copy_source_mailbox_id: Some(moving_folder.id),
            },
            postgres_mapi_audit("copy-notification-mailbox", moving_folder.id),
        )
        .await?;
    let copied = storage
        .poll_mapi_notifications(
            grantee_account_id,
            moved.cursor.expect("movement notification cursor"),
        )
        .await?;
    assert!(copied.event_pending);
    assert_eq!(copied.events.len(), 1);
    let copied_folder_id = copied.events[0]
        .notification_test_shape()
        .3
        .expect("copied folder ID");
    assert_eq!(
        copied.events[0].notification_test_shape(),
        (
            MapiNotificationKind::Hierarchy,
            0x0040,
            destination_parent_id,
            Some(copied_folder_id),
            Some(moving_folder_id),
            None,
            Some("mailbox"),
        )
    );
    assert_eq!(
        copied.events[0].old_parent_folder_id(),
        Some(destination_parent_id)
    );
    assert!(copied.events[0].is_complete_for_wire());
    let shared_checkpoint = storage
        .fetch_mapi_sync_checkpoint(
            grantee_account_id,
            Some(moving_folder.id),
            MapiCheckpointKind::Content,
        )
        .await?;
    assert_eq!(
        shared_checkpoint
            .as_ref()
            .map(|checkpoint| checkpoint.last_change_sequence),
        Some(7)
    );
    let allocated_grantee_identity_count = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*)
        FROM mapi_object_identities
        WHERE account_id = $1
          AND object_kind = 'mailbox'
          AND canonical_id = ANY($2)
          AND deleted_at IS NULL
        "#,
    )
    .bind(grantee_account_id)
    .bind(vec![
        source_parent.id,
        middle_parent.id,
        moving_folder.id,
        copied_folder.id,
    ])
    .fetch_one(storage.pool())
    .await?;
    assert_eq!(allocated_grantee_identity_count, 4);

    // [MS-OXCNOTIF] section 2.2.1.4.1.2 requires each hierarchy movement to
    // retain destination and source folder identities, even when the receiver
    // polls after multiple canonical updates.
    fixture.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn mapi_nested_folder_delete_replay_uses_historical_parent_and_retained_identity_in_postgresql(
) -> anyhow::Result<()> {
    let Some(fixture) = postgres_mapi_calendar_fixture().await? else {
        return Ok(());
    };
    let storage = fixture.storage.clone();
    let account_id = fixture.account_id;
    let parent = create_notification_mailbox(&storage, account_id, "Projects", None).await?;
    let child =
        create_notification_mailbox(&storage, account_id, "Quarterly reports", Some(parent.id))
            .await?;
    let identities = storage
        .fetch_or_allocate_mapi_identities(
            account_id,
            &[parent.id, child.id]
                .into_iter()
                .map(|canonical_id| MapiIdentityRequest {
                    object_kind: MapiIdentityObjectKind::Mailbox,
                    canonical_id,
                    reserved_global_counter: None,
                    source_key: None,
                })
                .collect::<Vec<_>>(),
        )
        .await?;
    let folder_ids = identities
        .into_iter()
        .map(|identity| (identity.canonical_id, identity.object_id))
        .collect::<HashMap<_, _>>();
    let baseline_cursor = storage
        .fetch_mapi_notification_cursor(account_id)
        .await?
        .unwrap_or(0);
    storage
        .destroy_jmap_mailbox(
            account_id,
            child.id,
            postgres_mapi_audit("delete-notification-mailbox", child.id),
        )
        .await?;
    let deleted_log = sqlx::query(
        r#"
        SELECT cursor, summary_json
        FROM mail_change_log
        WHERE account_id = $1
          AND object_kind = 'mailbox'
          AND object_id = $2
          AND change_kind = 'destroyed'
          AND cursor > $3
        ORDER BY cursor DESC
        LIMIT 1
        "#,
    )
    .bind(account_id)
    .bind(child.id)
    .bind(baseline_cursor)
    .fetch_one(storage.pool())
    .await?;
    let parent_id_string = parent.id.to_string();
    assert_eq!(
        deleted_log
            .get::<serde_json::Value, _>("summary_json")
            .get("parentId")
            .and_then(serde_json::Value::as_str),
        Some(parent_id_string.as_str())
    );
    sqlx::query(
        r#"
        UPDATE mapi_object_identities
        SET deleted_at = NOW()
        WHERE account_id = $1
          AND object_kind = 'mailbox'
          AND canonical_id = $2
        "#,
    )
    .bind(account_id)
    .bind(child.id)
    .execute(storage.pool())
    .await?;

    let deleted = storage
        .poll_mapi_notifications(account_id, baseline_cursor)
        .await?;
    assert!(deleted.event_pending);
    assert_eq!(deleted.cursor, Some(deleted_log.get("cursor")));
    assert_eq!(deleted.events.len(), 1);
    assert_eq!(
        deleted.events[0].notification_test_shape(),
        (
            MapiNotificationKind::Hierarchy,
            0x0008,
            folder_ids[&parent.id],
            Some(folder_ids[&child.id]),
            None,
            None,
            Some("mailbox"),
        )
    );
    let retained_identity_is_deleted = sqlx::query_scalar::<_, bool>(
        r#"
        SELECT deleted_at IS NOT NULL
        FROM mapi_object_identities
        WHERE account_id = $1
          AND object_kind = 'mailbox'
          AND canonical_id = $2
        "#,
    )
    .bind(account_id)
    .bind(child.id)
    .fetch_one(storage.pool())
    .await?;
    assert!(retained_identity_is_deleted);

    // [MS-OXCNOTIF] section 2.2.1.4.1.2 needs the parent FolderId and deleted
    // FolderId from the historical hierarchy state; polling must not revive a
    // tombstoned folder identity merely to construct a notification.
    fixture.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn mapi_imap_cross_parent_rename_replays_a_hierarchy_move_in_postgresql() -> anyhow::Result<()>
{
    let Some(fixture) = postgres_mapi_calendar_fixture().await? else {
        return Ok(());
    };
    let storage = fixture.storage.clone();
    let account_id = fixture.account_id;
    let source_parent = create_notification_mailbox(&storage, account_id, "Projects", None).await?;
    let destination_parent =
        create_notification_mailbox(&storage, account_id, "Client archive", None).await?;
    let renamed_folder = create_notification_mailbox(
        &storage,
        account_id,
        "Quarterly reports",
        Some(source_parent.id),
    )
    .await?;
    let identities = storage
        .fetch_or_allocate_mapi_identities(
            account_id,
            &[source_parent.id, destination_parent.id, renamed_folder.id]
                .into_iter()
                .map(|canonical_id| MapiIdentityRequest {
                    object_kind: MapiIdentityObjectKind::Mailbox,
                    canonical_id,
                    reserved_global_counter: None,
                    source_key: None,
                })
                .collect::<Vec<_>>(),
        )
        .await?;
    let folder_ids = identities
        .into_iter()
        .map(|identity| (identity.canonical_id, identity.object_id))
        .collect::<HashMap<_, _>>();
    let baseline_cursor = storage
        .fetch_mapi_notification_cursor(account_id)
        .await?
        .unwrap_or(0);
    storage
        .rename_imap_mailbox(
            account_id,
            renamed_folder.id,
            "Client archive/Quarterly reports renamed",
            postgres_mapi_audit("imap-cross-parent-rename", renamed_folder.id),
        )
        .await?;
    let renamed_log = sqlx::query(
        r#"
        SELECT cursor, change_kind, summary_json
        FROM mail_change_log
        WHERE account_id = $1
          AND object_kind = 'mailbox'
          AND object_id = $2
          AND cursor > $3
        ORDER BY cursor DESC
        LIMIT 1
        "#,
    )
    .bind(account_id)
    .bind(renamed_folder.id)
    .bind(baseline_cursor)
    .fetch_one(storage.pool())
    .await?;
    assert_eq!(renamed_log.get::<String, _>("change_kind"), "moved");
    let source_parent_id = source_parent.id.to_string();
    let destination_parent_id = destination_parent.id.to_string();
    let summary = renamed_log.get::<serde_json::Value, _>("summary_json");
    assert_eq!(
        summary
            .get("oldParentId")
            .and_then(serde_json::Value::as_str),
        Some(source_parent_id.as_str())
    );
    assert_eq!(
        summary.get("parentId").and_then(serde_json::Value::as_str),
        Some(destination_parent_id.as_str())
    );

    let moved = storage
        .poll_mapi_notifications(account_id, baseline_cursor)
        .await?;
    assert!(moved.event_pending);
    assert_eq!(moved.cursor, Some(renamed_log.get("cursor")));
    assert_eq!(moved.events.len(), 2);
    assert_eq!(
        moved.events[0].notification_test_shape(),
        (
            MapiNotificationKind::Hierarchy,
            0x0020,
            folder_ids[&destination_parent.id],
            Some(folder_ids[&renamed_folder.id]),
            Some(folder_ids[&renamed_folder.id]),
            None,
            Some("mailbox"),
        )
    );
    assert_eq!(
        moved.events[0].old_parent_folder_id(),
        Some(folder_ids[&source_parent.id])
    );
    assert_eq!(
        moved.events[1].notification_test_shape(),
        (
            MapiNotificationKind::Hierarchy,
            0x0100,
            folder_ids[&source_parent.id],
            Some(folder_ids[&renamed_folder.id]),
            None,
            None,
            None,
        )
    );

    fixture.cleanup().await?;
    Ok(())
}
