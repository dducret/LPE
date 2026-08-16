const FETCH_EWS_DELEGATES_SQL: &str = r#"
    WITH inbox_grants AS (
        SELECT grant_row.*
        FROM mailbox_delegation_grants grant_row
        JOIN mailboxes mailbox
          ON mailbox.tenant_id = grant_row.tenant_id
         AND mailbox.account_id = grant_row.owner_account_id
         AND mailbox.id = grant_row.mailbox_id
         AND mailbox.role = 'inbox'
        WHERE grant_row.tenant_id = $1
          AND grant_row.owner_account_id = $2
    ),
    default_calendar_grants AS (
        SELECT grant_row.*
        FROM calendar_grants grant_row
        JOIN calendars calendar
          ON calendar.tenant_id = grant_row.tenant_id
         AND calendar.owner_account_id = grant_row.owner_account_id
         AND calendar.id = grant_row.calendar_id
         AND calendar.role = 'calendar'
        WHERE grant_row.tenant_id = $1
          AND grant_row.owner_account_id = $2
    ),
    principals AS (
        SELECT grantee_account_id
        FROM inbox_grants
        UNION
        SELECT grantee_account_id
        FROM default_calendar_grants
        UNION
        SELECT grantee_account_id
        FROM sender_rights
        WHERE tenant_id = $1 AND owner_account_id = $2 AND identity_id IS NULL
        UNION
        SELECT grantee_account_id
        FROM delegate_preferences
        WHERE tenant_id = $1 AND owner_account_id = $2
    )
    SELECT
        p.grantee_account_id,
        grantee.primary_email AS grantee_email,
        grantee.display_name AS grantee_display_name,
        COALESCE(mail.may_read, FALSE) AS inbox_may_read,
        COALESCE(mail.may_write, FALSE) AS inbox_may_write,
        COALESCE(mail.may_delete, FALSE) AS inbox_may_delete,
        COALESCE(mail.may_share, FALSE) AS inbox_may_share,
        COALESCE(calendar.may_read, FALSE) AS calendar_may_read,
        COALESCE(calendar.may_write, FALSE) AS calendar_may_write,
        COALESCE(calendar.may_delete, FALSE) AS calendar_may_delete,
        COALESCE(calendar.may_share, FALSE) AS calendar_may_share,
        EXISTS (
            SELECT 1
            FROM sender_rights sender
            WHERE sender.tenant_id = $1
              AND sender.owner_account_id = $2
              AND sender.grantee_account_id = p.grantee_account_id
              AND sender.sender_right = 'send_on_behalf'
              AND sender.identity_id IS NULL
        ) AS may_send_on_behalf,
        EXISTS (
            SELECT 1
            FROM sender_rights sender
            WHERE sender.tenant_id = $1
              AND sender.owner_account_id = $2
              AND sender.grantee_account_id = p.grantee_account_id
              AND sender.sender_right = 'send_as'
              AND sender.identity_id IS NULL
        ) AS may_send_as,
        COALESCE(pref.meeting_request_delivery, 'delegate_and_owner') AS meeting_request_delivery,
        COALESCE(pref.receives_meeting_request_copy, TRUE) AS receives_meeting_request_copy,
        COALESCE(pref.may_view_private_items, FALSE) AS may_view_private_items
    FROM principals p
    JOIN accounts grantee
      ON grantee.tenant_id = $1
     AND grantee.id = p.grantee_account_id
    LEFT JOIN inbox_grants mail
      ON mail.tenant_id = $1
     AND mail.owner_account_id = $2
     AND mail.grantee_account_id = p.grantee_account_id
    LEFT JOIN default_calendar_grants calendar
      ON calendar.tenant_id = $1
     AND calendar.owner_account_id = $2
     AND calendar.grantee_account_id = p.grantee_account_id
    LEFT JOIN delegate_preferences pref
      ON pref.tenant_id = $1
     AND pref.owner_account_id = $2
     AND pref.grantee_account_id = p.grantee_account_id
    ORDER BY lower(grantee.primary_email) ASC
"#;

fn ews_delegates_from_rows(
    owner_account_id: Uuid,
    rows: Vec<sqlx::postgres::PgRow>,
) -> Result<Vec<EwsDelegate>> {
    rows.into_iter()
        .map(|row| {
            Ok(EwsDelegate {
                owner_account_id,
                grantee_account_id: row.try_get("grantee_account_id")?,
                grantee_email: row.try_get("grantee_email")?,
                grantee_display_name: row.try_get("grantee_display_name")?,
                inbox_rights: CollaborationRights {
                    may_read: row.try_get("inbox_may_read")?,
                    may_write: row.try_get("inbox_may_write")?,
                    may_delete: row.try_get("inbox_may_delete")?,
                    may_share: row.try_get("inbox_may_share")?,
                },
                calendar_rights: CollaborationRights {
                    may_read: row.try_get("calendar_may_read")?,
                    may_write: row.try_get("calendar_may_write")?,
                    may_delete: row.try_get("calendar_may_delete")?,
                    may_share: row.try_get("calendar_may_share")?,
                },
                may_send_on_behalf: row.try_get("may_send_on_behalf")?,
                may_send_as: row.try_get("may_send_as")?,
                preferences: EwsDelegatePreferences {
                    meeting_request_delivery: row.try_get("meeting_request_delivery")?,
                    receives_meeting_request_copy: row.try_get("receives_meeting_request_copy")?,
                    may_view_private_items: row.try_get("may_view_private_items")?,
                },
            })
        })
        .collect()
}

macro_rules! store_impl_ews_delegation {
    () => {
        fn upsert_ews_sharing_grant<'a>(
            &'a self,
            owner_account_id: Uuid,
            grantee_email: &'a str,
            kind: CollaborationResourceKind,
            rights: CollaborationRights,
            audit: AuditEntryInput,
        ) -> StoreFuture<'a, CollaborationGrant> {
            Box::pin(async move {
                Storage::upsert_collaboration_grant(
                    self,
                    CollaborationGrantInput {
                        kind,
                        owner_account_id,
                        grantee_email: grantee_email.to_string(),
                        calendar_id: None,
                        may_read: rights.may_read,
                        may_write: rights.may_write,
                        may_delete: rights.may_delete,
                        may_share: rights.may_share,
                    },
                    audit,
                )
                .await
            })
        }

        fn fetch_account_category_modseq<'a>(
            &'a self,
            account_id: Uuid,
            category: &'a str,
        ) -> StoreFuture<'a, u64> {
            Box::pin(async move {
                Storage::fetch_account_category_modseq(self, account_id, category).await
            })
        }

        fn fetch_ews_delegates<'a>(
            &'a self,
            owner_account_id: Uuid,
        ) -> StoreFuture<'a, Vec<EwsDelegate>> {
            Box::pin(async move {
                let tenant_id = mapi_tenant_id_for_account(self, owner_account_id).await?;
                let rows = sqlx::query(FETCH_EWS_DELEGATES_SQL)
                    .bind(tenant_id)
                    .bind(owner_account_id)
                    .fetch_all(self.pool())
                    .await?;
                ews_delegates_from_rows(owner_account_id, rows)
            })
        }

        fn apply_ews_delegate_batch<'a>(
            &'a self,
            inputs: &'a [UpsertEwsDelegateInput],
            audit: AuditEntryInput,
        ) -> StoreFuture<'a, Vec<EwsDelegate>> {
            Box::pin(async move {
                let owner_account_id = inputs
                    .first()
                    .map(|input| input.owner_account_id)
                    .ok_or_else(|| anyhow::anyhow!("delegate batch is empty"))?;
                if inputs.iter().any(|input| input.owner_account_id != owner_account_id) {
                    anyhow::bail!("delegate batch has mixed owners");
                }
                let canonical_inputs = inputs
                    .iter()
                    .map(|input| lpe_storage::CanonicalEwsDelegateInput {
                        owner_account_id: input.owner_account_id,
                        grantee_email: input.grantee_email.clone(),
                        inbox_may_read: input.inbox_rights.may_read,
                        inbox_may_write: input.inbox_rights.may_write,
                        inbox_may_delete: input.inbox_rights.may_delete,
                        inbox_may_share: input.inbox_rights.may_share,
                        calendar_may_read: input.calendar_rights.may_read,
                        calendar_may_write: input.calendar_rights.may_write,
                        calendar_may_delete: input.calendar_rights.may_delete,
                        calendar_may_share: input.calendar_rights.may_share,
                        may_send_on_behalf: input.may_send_on_behalf,
                        preferences: lpe_storage::DelegatePreferences {
                            meeting_request_delivery: input
                                .preferences
                                .meeting_request_delivery
                                .clone(),
                            receives_meeting_request_copy: input
                                .preferences
                                .receives_meeting_request_copy,
                            may_view_private_items: input.preferences.may_view_private_items,
                        },
                    })
                    .collect::<Vec<_>>();
                let grantee_ids = Storage::apply_canonical_ews_delegate_batch(
                    self,
                    owner_account_id,
                    &canonical_inputs,
                    audit,
                )
                .await?;
                let delegates = self.fetch_ews_delegates(owner_account_id).await?;
                grantee_ids
                    .into_iter()
                    .map(|grantee_id| {
                        delegates
                            .iter()
                            .find(|delegate| delegate.grantee_account_id == grantee_id)
                            .cloned()
                            .ok_or_else(|| anyhow::anyhow!("delegate not found after commit"))
                    })
                    .collect()
            })
        }

        fn remove_ews_delegate_batch<'a>(
            &'a self,
            owner_account_id: Uuid,
            grantee_account_ids: &'a [Uuid],
            audit: AuditEntryInput,
        ) -> StoreFuture<'a, ()> {
            Box::pin(async move {
                Storage::remove_canonical_ews_delegate_batch(
                    self,
                    owner_account_id,
                    grantee_account_ids,
                    audit,
                )
                .await
            })
        }
    };
}
