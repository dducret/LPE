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

    fn fetch_ews_delegates<'a>(
        &'a self,
        owner_account_id: Uuid,
    ) -> StoreFuture<'a, Vec<EwsDelegate>> {
        Box::pin(async move {
            let tenant_id = mapi_tenant_id_for_account(self, owner_account_id).await?;
            let rows = sqlx::query(
                r#"
                WITH principals AS (
                    SELECT grantee_account_id
                    FROM mailbox_delegation_grants
                    WHERE tenant_id = $1 AND owner_account_id = $2
                    UNION
                    SELECT grantee_account_id
                    FROM calendar_grants
                    WHERE tenant_id = $1 AND owner_account_id = $2
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
                LEFT JOIN mailbox_delegation_grants mail
                  ON mail.tenant_id = $1
                 AND mail.owner_account_id = $2
                 AND mail.grantee_account_id = p.grantee_account_id
                LEFT JOIN calendar_grants calendar
                  ON calendar.tenant_id = $1
                 AND calendar.owner_account_id = $2
                 AND calendar.grantee_account_id = p.grantee_account_id
                LEFT JOIN delegate_preferences pref
                  ON pref.tenant_id = $1
                 AND pref.owner_account_id = $2
                 AND pref.grantee_account_id = p.grantee_account_id
                ORDER BY lower(grantee.primary_email) ASC
                "#,
            )
            .bind(tenant_id)
            .bind(owner_account_id)
            .fetch_all(self.pool())
            .await?;

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
                            receives_meeting_request_copy: row
                                .try_get("receives_meeting_request_copy")?,
                            may_view_private_items: row.try_get("may_view_private_items")?,
                        },
                    })
                })
                .collect()
        })
    }

    fn upsert_ews_delegate<'a>(
        &'a self,
        input: UpsertEwsDelegateInput,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, EwsDelegate> {
        Box::pin(async move {
            let tenant_id = mapi_tenant_id_for_account(self, input.owner_account_id).await?;
            let grantee_account_id = sqlx::query_scalar::<_, Uuid>(
                r#"
                SELECT id
                FROM accounts
                WHERE tenant_id = $1
                  AND normalized_primary_email = lower(btrim($2))
                  AND status = 'active'
                LIMIT 1
                "#,
            )
            .bind(tenant_id)
            .bind(&input.grantee_email)
            .fetch_optional(self.pool())
            .await?
            .ok_or_else(|| anyhow::anyhow!("delegate account not found in tenant"))?;

            if input.inbox_rights.may_read {
                Storage::upsert_mailbox_delegation_grant(
                    self,
                    MailboxDelegationGrantInput {
                        owner_account_id: input.owner_account_id,
                        grantee_email: input.grantee_email.clone(),
                        may_write: input.inbox_rights.may_write,
                    },
                    audit.clone(),
                )
                .await?;
            } else {
                let _ = Storage::delete_mailbox_delegation_grant(
                    self,
                    input.owner_account_id,
                    grantee_account_id,
                    audit.clone(),
                )
                .await;
            }

            if input.calendar_rights.may_read {
                Storage::upsert_collaboration_grant(
                    self,
                    CollaborationGrantInput {
                        kind: CollaborationResourceKind::Calendar,
                        owner_account_id: input.owner_account_id,
                        grantee_email: input.grantee_email.clone(),
                        calendar_id: None,
                        may_read: input.calendar_rights.may_read,
                        may_write: input.calendar_rights.may_write,
                        may_delete: input.calendar_rights.may_delete,
                        may_share: input.calendar_rights.may_share,
                    },
                    audit.clone(),
                )
                .await?;
            } else {
                let _ = Storage::delete_collaboration_grant(
                    self,
                    input.owner_account_id,
                    CollaborationResourceKind::Calendar,
                    grantee_account_id,
                    audit.clone(),
                )
                .await;
            }

            if input.may_send_on_behalf {
                Storage::upsert_sender_delegation_grant(
                    self,
                    SenderDelegationGrantInput {
                        owner_account_id: input.owner_account_id,
                        grantee_email: input.grantee_email.clone(),
                        sender_right: SenderDelegationRight::SendOnBehalf,
                    },
                    audit.clone(),
                )
                .await?;
            } else {
                let _ = Storage::delete_sender_delegation_grant(
                    self,
                    input.owner_account_id,
                    grantee_account_id,
                    SenderDelegationRight::SendOnBehalf,
                    audit.clone(),
                )
                .await;
            }

            sqlx::query(
                r#"
                INSERT INTO delegate_preferences (
                    tenant_id, owner_account_id, grantee_account_id,
                    meeting_request_delivery, receives_meeting_request_copy,
                    may_view_private_items
                )
                VALUES ($1, $2, $3, $4, $5, $6)
                ON CONFLICT (tenant_id, owner_account_id, grantee_account_id)
                DO UPDATE SET
                    meeting_request_delivery = EXCLUDED.meeting_request_delivery,
                    receives_meeting_request_copy = EXCLUDED.receives_meeting_request_copy,
                    may_view_private_items = EXCLUDED.may_view_private_items,
                    updated_at = NOW()
                "#,
            )
            .bind(tenant_id)
            .bind(input.owner_account_id)
            .bind(grantee_account_id)
            .bind(&input.preferences.meeting_request_delivery)
            .bind(input.preferences.receives_meeting_request_copy)
            .bind(input.preferences.may_view_private_items)
            .execute(self.pool())
            .await?;

            self.fetch_ews_delegates(input.owner_account_id)
                .await?
                .into_iter()
                .find(|delegate| delegate.grantee_account_id == grantee_account_id)
                .ok_or_else(|| anyhow::anyhow!("delegate not found after upsert"))
        })
    }

    fn remove_ews_delegate<'a>(
        &'a self,
        owner_account_id: Uuid,
        grantee_account_id: Uuid,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, bool> {
        Box::pin(async move {
            let tenant_id = mapi_tenant_id_for_account(self, owner_account_id).await?;
            let mut deleted = false;
            if Storage::delete_mailbox_delegation_grant(
                self,
                owner_account_id,
                grantee_account_id,
                audit.clone(),
            )
            .await
            .is_ok()
            {
                deleted = true;
            }
            if Storage::delete_collaboration_grant(
                self,
                owner_account_id,
                CollaborationResourceKind::Calendar,
                grantee_account_id,
                audit.clone(),
            )
            .await
            .is_ok()
            {
                deleted = true;
            }
            for right in [
                SenderDelegationRight::SendOnBehalf,
                SenderDelegationRight::SendAs,
            ] {
                if Storage::delete_sender_delegation_grant(
                    self,
                    owner_account_id,
                    grantee_account_id,
                    right,
                    audit.clone(),
                )
                .await
                .is_ok()
                {
                    deleted = true;
                }
            }
            let result = sqlx::query(
                r#"
                DELETE FROM delegate_preferences
                WHERE tenant_id = $1
                  AND owner_account_id = $2
                  AND grantee_account_id = $3
                "#,
            )
            .bind(tenant_id)
            .bind(owner_account_id)
            .bind(grantee_account_id)
            .execute(self.pool())
            .await?;
            Ok(deleted || result.rows_affected() > 0)
        })
    }

    };
}
