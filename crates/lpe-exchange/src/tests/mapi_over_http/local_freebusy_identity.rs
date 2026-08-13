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
