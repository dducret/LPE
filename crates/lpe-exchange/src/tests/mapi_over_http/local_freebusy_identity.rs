use super::*;

async fn postgres_local_freebusy_identity(
    storage: &Storage,
    account_id: Uuid,
) -> anyhow::Result<MapiIdentityRecord> {
    let snapshot = storage.load_mapi_mail_store(account_id, 500).await?;
    snapshot
        .delegate_freebusy_messages()
        .iter()
        .find(|message| {
            message.canonical_id == crate::mapi_store::OUTLOOK_LOCAL_FREEBUSY_CANONICAL_ID
        })
        .and_then(|message| message.durable_identity.clone())
        .ok_or_else(|| anyhow::anyhow!("LocalFreebusy is missing its durable MAPI identity"))
}

async fn postgres_local_freebusy_projection(
    storage: &Storage,
    account_id: Uuid,
) -> anyhow::Result<(MapiIdentityRecord, Vec<EwsDelegate>)> {
    let snapshot = storage.load_mapi_mail_store(account_id, 500).await?;
    let message = snapshot
        .delegate_freebusy_messages()
        .iter()
        .find(|message| {
            message.canonical_id == crate::mapi_store::OUTLOOK_LOCAL_FREEBUSY_CANONICAL_ID
        })
        .ok_or_else(|| anyhow::anyhow!("canonical MAPI LocalFreebusy message is missing"))?;
    Ok((
        message
            .durable_identity
            .clone()
            .ok_or_else(|| anyhow::anyhow!("LocalFreebusy is missing its durable MAPI identity"))?,
        message.delegates.clone(),
    ))
}

#[tokio::test]
async fn postgres_local_freebusy_identity_rotates_only_for_default_delegate_state(
) -> anyhow::Result<()> {
    let Some(fixture) = postgres_mapi_calendar_fixture().await? else {
        return Ok(());
    };
    let storage = fixture.storage.clone();
    let tenant_id = Uuid::parse_str("10000000-0000-0000-0000-000000000001").unwrap();
    let domain_id = Uuid::parse_str("10000000-0000-0000-0000-000000000002").unwrap();
    let delegate_account_id = Uuid::parse_str("10000000-0000-0000-0000-000000000008").unwrap();
    sqlx::query(
        r#"
        INSERT INTO accounts (id, tenant_id, primary_domain_id, primary_email, display_name)
        VALUES ($1, $2, $3, 'identity-delegate@example.test', 'Identity Delegate')
        "#,
    )
    .bind(delegate_account_id)
    .bind(tenant_id)
    .bind(domain_id)
    .execute(storage.pool())
    .await?;

    let generic_local_identity = [crate::mapi_store::outlook_local_freebusy_identity_request()];
    let error = storage
        .fetch_or_allocate_mapi_identities(fixture.account_id, &generic_local_identity)
        .await
        .expect_err("generic identity allocation accepted LocalFreebusy");
    assert!(error.to_string().contains("canonical delegate projection"));

    let initial = postgres_local_freebusy_identity(&storage, fixture.account_id).await?;
    let freebusy_mailbox_id = crate::mapi_mailstore::virtual_special_mailbox(
        crate::mapi::identity::FREEBUSY_DATA_FOLDER_ID,
    )
    .expect("Freebusy Data virtual mailbox")
    .id;
    let before_seed_cursor = storage
        .fetch_mapi_notification_cursor(fixture.account_id)
        .await?
        .unwrap_or(0);
    storage
        .upsert_mailbox_delegation_grant_with_preferences(
            lpe_storage::MailboxDelegationGrantInput {
                owner_account_id: fixture.account_id,
                grantee_email: "identity-delegate@example.test".to_string(),
                may_write: true,
            },
            lpe_storage::DelegatePreferencesPatch {
                receives_meeting_request_copy: Some(true),
                ..Default::default()
            },
            postgres_mapi_audit("seed-local-freebusy-delegate", delegate_account_id),
        )
        .await?;
    let seeded = postgres_local_freebusy_identity(&storage, fixture.account_id).await?;
    let reloaded = postgres_local_freebusy_identity(&storage, fixture.account_id).await?;
    assert_eq!(
        seeded, reloaded,
        "an unchanged reload rotated LocalFreebusy"
    );
    assert_eq!(initial.object_id, seeded.object_id);
    assert_eq!(initial.source_key, seeded.source_key);
    let seed_changes = storage
        .fetch_mapi_sync_changes(
            fixture.account_id,
            Some(freebusy_mailbox_id),
            MapiCheckpointKind::Content,
            before_seed_cursor as u64,
        )
        .await?;
    assert_eq!(
        seed_changes.changed_delegate_freebusy_ids,
        vec![crate::mapi_store::OUTLOOK_LOCAL_FREEBUSY_CANONICAL_ID],
        "one delegate-driven rotation must produce one Freebusy Data ICS delta"
    );
    let seed_poll = storage
        .poll_mapi_notifications(fixture.account_id, before_seed_cursor)
        .await?;
    let seed_notifications = seed_poll
        .events
        .iter()
        .filter(|event| event.notification_test_shape().6 == Some("delegate_freebusy_message"))
        .collect::<Vec<_>>();
    assert_eq!(seed_notifications.len(), 1);
    assert_eq!(
        seed_notifications[0].notification_test_shape(),
        (
            MapiNotificationKind::Content,
            MAPI_OBJECT_MODIFIED_EVENT_MASK,
            crate::mapi::identity::FREEBUSY_DATA_FOLDER_ID,
            Some(initial.object_id),
            None,
            None,
            Some("delegate_freebusy_message"),
        ),
        "the durable delegate-driven rotation must be replayable by another MAPI session"
    );

    storage
        .upsert_mailbox_delegation_grant_with_preferences(
            lpe_storage::MailboxDelegationGrantInput {
                owner_account_id: fixture.account_id,
                grantee_email: "identity-delegate@example.test".to_string(),
                may_write: true,
            },
            lpe_storage::DelegatePreferencesPatch {
                receives_meeting_request_copy: Some(true),
                ..Default::default()
            },
            postgres_mapi_audit("repeat-local-freebusy-delegate", delegate_account_id),
        )
        .await?;
    let idempotent_upsert = postgres_local_freebusy_identity(&storage, fixture.account_id).await?;
    assert_eq!(
        seeded, idempotent_upsert,
        "an idempotent delegate upsert rotated LocalFreebusy"
    );

    storage
        .upsert_mailbox_delegation_grant_with_preferences(
            lpe_storage::MailboxDelegationGrantInput {
                owner_account_id: fixture.account_id,
                grantee_email: "identity-delegate@example.test".to_string(),
                may_write: true,
            },
            lpe_storage::DelegatePreferencesPatch {
                may_view_private_items: Some(true),
                ..Default::default()
            },
            postgres_mapi_audit("update-local-freebusy-preference", delegate_account_id),
        )
        .await?;
    let preference_update = postgres_local_freebusy_identity(&storage, fixture.account_id).await?;
    assert_eq!(idempotent_upsert.object_id, preference_update.object_id);
    assert_eq!(idempotent_upsert.source_key, preference_update.source_key);
    assert!(preference_update.change_number > idempotent_upsert.change_number);
    assert_ne!(preference_update.change_key, idempotent_upsert.change_key);
    assert_eq!(
        &preference_update.change_key[..16],
        &idempotent_upsert.change_key[..16]
    );
    assert!(
        preference_update
            .predecessor_change_list
            .windows(preference_update.change_key.len())
            .any(|candidate| candidate == preference_update.change_key),
        "the rotated PCL does not dominate the previous LocalFreebusy ChangeKey"
    );
    assert!(preference_update.last_modification_time > idempotent_upsert.last_modification_time);

    sqlx::query(
        r#"
        UPDATE accounts
        SET primary_email = 'renamed-identity-delegate@example.test',
            display_name = 'Renamed Identity Delegate',
            updated_at = NOW()
        WHERE tenant_id = $1
          AND id = $2
        "#,
    )
    .bind(tenant_id)
    .bind(delegate_account_id)
    .execute(storage.pool())
    .await?;
    let (directory_update, delegates) =
        postgres_local_freebusy_projection(&storage, fixture.account_id).await?;
    assert_eq!(preference_update.object_id, directory_update.object_id);
    assert_eq!(preference_update.source_key, directory_update.source_key);
    assert!(directory_update.change_number > preference_update.change_number);
    assert_ne!(directory_update.change_key, preference_update.change_key);
    assert_eq!(
        &directory_update.change_key[..16],
        &preference_update.change_key[..16]
    );
    assert!(
        directory_update
            .predecessor_change_list
            .windows(directory_update.change_key.len())
            .any(|candidate| candidate == directory_update.change_key),
        "the directory-triggered PCL does not dominate the prior LocalFreebusy ChangeKey"
    );
    assert_eq!(delegates.len(), 1);
    assert_eq!(
        delegates[0].grantee_email,
        "renamed-identity-delegate@example.test"
    );
    assert_eq!(
        delegates[0].grantee_display_name,
        "Renamed Identity Delegate"
    );
    let (revision, applied_revision) = sqlx::query_as::<_, (i64, i64)>(
        r#"
        SELECT revision, applied_revision
        FROM delegation_projection_state
        WHERE tenant_id = $1
          AND account_id = $2
        "#,
    )
    .bind(tenant_id)
    .bind(fixture.account_id)
    .fetch_one(storage.pool())
    .await?;
    assert_eq!(revision, applied_revision);

    let before_calendar_cursor = storage
        .fetch_mapi_notification_cursor(fixture.account_id)
        .await?
        .unwrap_or(0);
    storage
        .upsert_client_event(UpsertClientEventInput {
            id: Some(Uuid::parse_str("78787878-7878-4878-9878-787878787878").unwrap()),
            account_id: fixture.account_id,
            uid: "local-freebusy-identity-calendar-control".to_string(),
            date: "2026-08-13".to_string(),
            time: "12:00".to_string(),
            time_zone: "UTC".to_string(),
            duration_minutes: 30,
            all_day: false,
            status: "confirmed".to_string(),
            sequence: 0,
            recurrence_rule: String::new(),
            recurrence_json: "{}".to_string(),
            recurrence_exceptions_json: "[]".to_string(),
            title: "Calendar event must not rotate LocalFreebusy".to_string(),
            location: String::new(),
            organizer_json: "{}".to_string(),
            attendees: String::new(),
            attendees_json: "{}".to_string(),
            notes: String::new(),
            body_html: String::new(),
        })
        .await?;
    let after_event = postgres_local_freebusy_identity(&storage, fixture.account_id).await?;
    assert_eq!(
        directory_update, after_event,
        "ordinary Calendar Event state rotated LocalFreebusy"
    );
    let calendar_control_changes = storage
        .fetch_mapi_sync_changes(
            fixture.account_id,
            Some(freebusy_mailbox_id),
            MapiCheckpointKind::Content,
            before_calendar_cursor as u64,
        )
        .await?;
    assert!(
        calendar_control_changes
            .changed_delegate_freebusy_ids
            .is_empty(),
        "ordinary Calendar churn leaked into Freebusy Data ICS"
    );
    let calendar_control_poll = storage
        .poll_mapi_notifications(fixture.account_id, before_calendar_cursor)
        .await?;
    assert!(
        calendar_control_poll.events.iter().all(|event| {
            event.notification_test_shape().6 != Some("delegate_freebusy_message")
        }),
        "ordinary Calendar churn emitted a LocalFreebusy notification"
    );

    storage
        .delete_mailbox_delegation_grant(
            fixture.account_id,
            delegate_account_id,
            postgres_mapi_audit("delete-last-local-freebusy-delegate", delegate_account_id),
        )
        .await?;
    let last_delete = postgres_local_freebusy_identity(&storage, fixture.account_id).await?;
    assert_eq!(after_event.object_id, last_delete.object_id);
    assert_eq!(after_event.source_key, last_delete.source_key);
    assert!(last_delete.change_number > after_event.change_number);
    assert_ne!(last_delete.change_key, after_event.change_key);
    assert!(last_delete.last_modification_time > after_event.last_modification_time);

    fixture.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn postgres_local_freebusy_custom_saves_return_one_authoritative_bag_and_replay_notification(
) -> anyhow::Result<()> {
    let Some(fixture) = postgres_mapi_calendar_fixture().await? else {
        return Ok(());
    };
    let storage = fixture.storage.clone();
    let account_id = fixture.account_id;
    let tenant_id = Uuid::parse_str("10000000-0000-0000-0000-000000000001").unwrap();
    let domain_id = Uuid::parse_str("10000000-0000-0000-0000-000000000002").unwrap();
    let delegate_account_id = Uuid::parse_str("10000000-0000-0000-0000-000000000008").unwrap();
    sqlx::query(
        r#"
        INSERT INTO accounts (id, tenant_id, primary_domain_id, primary_email, display_name)
        VALUES ($1, $2, $3, 'custom-save-delegate@example.test', 'Custom Save Delegate')
        "#,
    )
    .bind(delegate_account_id)
    .bind(tenant_id)
    .bind(domain_id)
    .execute(storage.pool())
    .await?;
    let initial = postgres_local_freebusy_identity(&storage, account_id).await?;
    let baseline_cursor = storage
        .fetch_mapi_notification_cursor(account_id)
        .await?
        .unwrap_or(0);
    let first_tag = 0x9100_000B;
    let second_tag = 0x9101_000B;

    storage
        .upsert_mailbox_delegation_grant_with_preferences(
            lpe_storage::MailboxDelegationGrantInput {
                owner_account_id: account_id,
                grantee_email: "custom-save-delegate@example.test".to_string(),
                may_write: true,
            },
            lpe_storage::DelegatePreferencesPatch {
                receives_meeting_request_copy: Some(true),
                ..Default::default()
            },
            postgres_mapi_audit("pending-local-freebusy-delegate", delegate_account_id),
        )
        .await?;
    let pending_projection_state = sqlx::query_as::<_, (i64, i64)>(
        r#"
        SELECT revision, applied_revision
        FROM delegation_projection_state
        WHERE tenant_id = $1 AND account_id = $2
        "#,
    )
    .bind(tenant_id)
    .bind(account_id)
    .fetch_one(storage.pool())
    .await?;
    assert!(pending_projection_state.0 > pending_projection_state.1);

    let first = storage
        .commit_local_freebusy_custom_property_changes(
            account_id,
            &[MapiCustomPropertyValue {
                property_tag: first_tag,
                property_type: 0x000B,
                property_value: vec![1],
            }],
            &[],
        )
        .await?;
    assert_eq!(first.delegates.len(), 1);
    assert_eq!(first.delegates[0].grantee_account_id, delegate_account_id);
    assert_eq!(
        first.delegates[0].grantee_email,
        "custom-save-delegate@example.test"
    );
    let applied_projection_state = sqlx::query_as::<_, (i64, i64)>(
        r#"
        SELECT revision, applied_revision
        FROM delegation_projection_state
        WHERE tenant_id = $1 AND account_id = $2
        "#,
    )
    .bind(tenant_id)
    .bind(account_id)
    .fetch_one(storage.pool())
    .await?;
    assert_eq!(applied_projection_state.0, applied_projection_state.1);
    assert_eq!(applied_projection_state.0, pending_projection_state.0);
    let cursor_after_first = storage
        .fetch_mapi_notification_cursor(account_id)
        .await?
        .unwrap_or(0);
    let (reloaded_after_first, reloaded_delegates) =
        postgres_local_freebusy_projection(&storage, account_id).await?;
    assert_eq!(reloaded_after_first, first.identity);
    assert_eq!(reloaded_delegates.len(), 1);
    assert_eq!(
        reloaded_delegates[0].grantee_account_id,
        delegate_account_id
    );
    assert_eq!(
        storage
            .fetch_mapi_notification_cursor(account_id)
            .await?
            .unwrap_or(0),
        cursor_after_first,
        "reloading the coalesced revision emitted a second journal row"
    );
    let coalesced_journal_rows = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*)
        FROM mail_change_log
        WHERE tenant_id = $1
          AND account_id = $2
          AND object_kind = 'delegate_freebusy_message'
          AND modseq = $3
        "#,
    )
    .bind(tenant_id)
    .bind(account_id)
    .bind(first.identity.change_number as i64)
    .fetch_one(storage.pool())
    .await?;
    assert_eq!(coalesced_journal_rows, 1);
    let second = storage
        .commit_local_freebusy_custom_property_changes(
            account_id,
            &[MapiCustomPropertyValue {
                property_tag: second_tag,
                property_type: 0x000B,
                property_value: vec![0],
            }],
            &[],
        )
        .await?;

    assert!(first.identity.change_number > initial.change_number);
    assert!(second.identity.change_number > first.identity.change_number);
    assert_eq!(second.delegates.len(), 1);
    assert_eq!(second.delegates[0].grantee_account_id, delegate_account_id);
    assert_eq!(
        second.custom_properties,
        vec![
            MapiCustomPropertyValue {
                property_tag: first_tag,
                property_type: 0x000B,
                property_value: vec![1],
            },
            MapiCustomPropertyValue {
                property_tag: second_tag,
                property_type: 0x000B,
                property_value: vec![0],
            },
        ],
        "the second save must return the complete bag committed under its ChangeKey"
    );

    let freebusy_mailbox_id = crate::mapi_mailstore::virtual_special_mailbox(
        crate::mapi::identity::FREEBUSY_DATA_FOLDER_ID,
    )
    .expect("Freebusy Data virtual mailbox")
    .id;
    let changes = storage
        .fetch_mapi_sync_changes(
            account_id,
            Some(freebusy_mailbox_id),
            MapiCheckpointKind::Content,
            baseline_cursor as u64,
        )
        .await?;
    assert_eq!(
        changes.changed_delegate_freebusy_ids,
        vec![crate::mapi_store::OUTLOOK_LOCAL_FREEBUSY_CANONICAL_ID],
        "incremental Freebusy Data ICS must consume the journaled LocalFreebusy version"
    );

    let poll = storage
        .poll_mapi_notifications(account_id, baseline_cursor)
        .await?;
    let local_freebusy_events = poll
        .events
        .iter()
        .filter(|event| event.notification_test_shape().6 == Some("delegate_freebusy_message"))
        .collect::<Vec<_>>();
    assert_eq!(local_freebusy_events.len(), 2);
    for event in local_freebusy_events {
        assert_eq!(
            event.notification_test_shape(),
            (
                MapiNotificationKind::Content,
                MAPI_OBJECT_MODIFIED_EVENT_MASK,
                crate::mapi::identity::FREEBUSY_DATA_FOLDER_ID,
                Some(initial.object_id),
                None,
                None,
                Some("delegate_freebusy_message"),
            )
        );
    }

    let paging_baseline = storage
        .fetch_mapi_notification_cursor(account_id)
        .await?
        .unwrap_or(0);
    sqlx::query(
        r#"
        WITH generated AS (
            SELECT
                ordinal,
                md5($2::text || ':mapi-sync-cursor:' || ordinal::text)::uuid AS id
            FROM generate_series(1, 1001) AS ordinal
        )
        INSERT INTO mapi_associated_config_messages (
            tenant_id, id, account_id, folder_id, message_class, subject, properties_json
        )
        SELECT
            $1, id, $2, $3, 'IPM.Configuration.CursorFlood',
            'Cursor flood ' || ordinal::text, '{}'::jsonb
        FROM generated
        "#,
    )
    .bind(tenant_id)
    .bind(account_id)
    .bind(crate::mapi::identity::COMMON_VIEWS_FOLDER_ID as i64)
    .execute(storage.pool())
    .await?;
    sqlx::query(
        r#"
        INSERT INTO mail_change_log (
            tenant_id, account_id, object_kind, object_id, change_kind, modseq,
            affected_principal_ids, summary_json
        )
        SELECT
            $1, $2, 'associated_config', id, 'updated',
            20000 + row_number() OVER (ORDER BY id)::bigint, ARRAY[$2]::uuid[],
            jsonb_build_object('folderId', $3::text)
        FROM mapi_associated_config_messages
        WHERE tenant_id = $1
          AND account_id = $2
          AND message_class = 'IPM.Configuration.CursorFlood'
        "#,
    )
    .bind(tenant_id)
    .bind(account_id)
    .bind(crate::mapi::identity::COMMON_VIEWS_FOLDER_ID as i64)
    .execute(storage.pool())
    .await?;
    storage
        .commit_local_freebusy_custom_property_changes(
            account_id,
            &[MapiCustomPropertyValue {
                property_tag: 0x9102_000B,
                property_type: 0x000B,
                property_value: vec![1],
            }],
            &[],
        )
        .await?;
    let tail_cursor = storage
        .fetch_mapi_notification_cursor(account_id)
        .await?
        .unwrap_or(0);
    let complete_page = storage
        .fetch_mapi_sync_changes(
            account_id,
            Some(freebusy_mailbox_id),
            MapiCheckpointKind::Content,
            paging_baseline as u64,
        )
        .await?;
    assert!(complete_page.changed_associated_config_ids.is_empty());
    assert_eq!(
        complete_page.changed_delegate_freebusy_ids,
        vec![crate::mapi_store::OUTLOOK_LOCAL_FREEBUSY_CANONICAL_ID],
        "more than one storage page of Common Views rows must not hide the later LocalFreebusy change"
    );
    assert_eq!(complete_page.current_change_sequence, tail_cursor as u64);
    let after_complete_page = storage
        .fetch_mapi_sync_changes(
            account_id,
            Some(freebusy_mailbox_id),
            MapiCheckpointKind::Content,
            complete_page.current_change_sequence,
        )
        .await?;
    assert!(after_complete_page.changed_delegate_freebusy_ids.is_empty());

    fixture.cleanup().await?;
    Ok(())
}
