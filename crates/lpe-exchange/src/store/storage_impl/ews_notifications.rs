macro_rules! store_impl_ews_notifications {
    () => {
        fn replay_ews_notification_events<'a>(
            &'a self,
            account_id: Uuid,
            after_cursor: i64,
            scope: &'a EwsNotificationFolderScope,
            event_types: &'a [EwsNotificationEventType],
            limit: usize,
        ) -> StoreFuture<'a, EwsNotificationReplay> {
            Box::pin(async move {
                let tenant_id = mapi_tenant_id_for_account(self, account_id).await?;
                let mut tx = self.pool().begin().await?;
                sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
                    .execute(&mut *tx)
                    .await?;
                let mailbox_ids = match scope {
                    EwsNotificationFolderScope::All => None,
                    EwsNotificationFolderScope::Mailboxes(ids) => Some(ids.as_slice()),
                };
                let (earliest_cursor, current_cursor): (Option<i64>, Option<i64>) = sqlx::query_as(
                    r#"
                    SELECT MIN(cursor), MAX(cursor)
                    FROM mail_change_log
                    WHERE tenant_id = $1
                      AND (account_id = $2 OR affected_principal_ids @> ARRAY[$2]::uuid[])
                      AND object_kind = 'mailbox_message'
                      AND NOT COALESCE((summary_json->>'mapiOnly')::boolean, FALSE)
                      AND ($3::uuid[] IS NULL OR mailbox_id = ANY($3))
                      AND (retained_until IS NULL OR retained_until > NOW())
                    "#,
                )
                .bind(tenant_id)
                .bind(account_id)
                .bind(mailbox_ids)
                .fetch_one(&mut *tx)
                .await?;
                let expired = after_cursor > 0
                    && earliest_cursor
                        .map(|cursor| after_cursor < cursor.saturating_sub(1))
                        .unwrap_or(true);
                if expired {
                    tx.commit().await?;
                    return Ok(EwsNotificationReplay {
                        expired: true,
                        current_cursor,
                        next_cursor: after_cursor,
                        more_events: false,
                        events: Vec::new(),
                    });
                }
                let include_created = event_types.iter().any(|event_type| {
                    matches!(
                        event_type,
                        EwsNotificationEventType::Created | EwsNotificationEventType::NewMail
                    )
                });
                let include_updated = event_types
                    .iter()
                    .any(|event_type| matches!(event_type, EwsNotificationEventType::Modified));
                let include_destroyed = event_types
                    .iter()
                    .any(|event_type| matches!(event_type, EwsNotificationEventType::Deleted));
                let rows = sqlx::query(
                    r#"
                SELECT
                    cursor,
                    mailbox_id,
                    NULLIF(summary_json->>'messageId', '')::uuid AS message_id,
                    change_kind,
                    modseq,
                    to_char(
                        created_at AT TIME ZONE 'UTC',
                        'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'
                    ) AS created_at
                FROM mail_change_log
                WHERE tenant_id = $1
                  AND (account_id = $2 OR affected_principal_ids @> ARRAY[$2]::uuid[])
                  AND object_kind = 'mailbox_message'
                  AND NOT COALESCE((summary_json->>'mapiOnly')::boolean, FALSE)
                  AND cursor > $3
                  AND ($4::bigint IS NULL OR cursor <= $4)
                  AND ($5::uuid[] IS NULL OR mailbox_id = ANY($5))
                  AND (retained_until IS NULL OR retained_until > NOW())
                  AND (
                        (change_kind = 'created' AND $6)
                     OR (change_kind = 'updated' AND $7)
                     OR (change_kind IN ('destroyed', 'expunged') AND $8)
                  )
                ORDER BY cursor ASC
                LIMIT $9
                "#,
                )
                .bind(tenant_id)
                .bind(account_id)
                .bind(after_cursor)
                .bind(current_cursor)
                .bind(mailbox_ids)
                .bind(include_created)
                .bind(include_updated)
                .bind(include_destroyed)
                .bind((limit.max(1) + 1) as i64)
                .fetch_all(&mut *tx)
                .await?;
                tx.commit().await?;
                let more_events = rows.len() > limit.max(1);
                let events = rows
                    .into_iter()
                    .take(limit.max(1))
                    .map(|row| {
                        Ok(EwsNotificationLogEvent {
                            cursor: row.try_get("cursor")?,
                            mailbox_id: row.try_get("mailbox_id")?,
                            message_id: row.try_get("message_id")?,
                            change_kind: row.try_get("change_kind")?,
                            modseq: row.try_get::<i64, _>("modseq")?.max(0) as u64,
                            created_at: row.try_get("created_at")?,
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;
                let next_cursor = events
                    .last()
                    .map(|event| event.cursor)
                    .or(current_cursor)
                    .unwrap_or(after_cursor);
                Ok(EwsNotificationReplay {
                    expired: false,
                    current_cursor,
                    next_cursor,
                    more_events,
                    events,
                })
            })
        }

        fn replay_ews_mailbox_item_sync<'a>(
            &'a self,
            account_id: Uuid,
            mailbox_id: Uuid,
            after_cursor: i64,
            limit: usize,
        ) -> StoreFuture<'a, EwsNotificationReplay> {
            Box::pin(async move {
                let tenant_id = mapi_tenant_id_for_account(self, account_id).await?;
                let mut tx = self.pool().begin().await?;
                sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
                    .execute(&mut *tx)
                    .await?;
                let (earliest_cursor, current_cursor): (Option<i64>, Option<i64>) = sqlx::query_as(
                    r#"
                    SELECT MIN(cursor), MAX(cursor)
                    FROM mail_change_log
                    WHERE tenant_id = $1
                      AND (account_id = $2 OR affected_principal_ids @> ARRAY[$2]::uuid[])
                      AND mailbox_id = $3
                      AND object_kind = 'mailbox_message'
                      AND NOT COALESCE((summary_json->>'mapiOnly')::boolean, FALSE)
                      AND (retained_until IS NULL OR retained_until > NOW())
                    "#,
                )
                .bind(tenant_id)
                .bind(account_id)
                .bind(mailbox_id)
                .fetch_one(&mut *tx)
                .await?;
                let expired = after_cursor > 0
                    && earliest_cursor
                        .map(|cursor| after_cursor < cursor.saturating_sub(1))
                        .unwrap_or(true);
                if expired {
                    tx.commit().await?;
                    return Ok(EwsNotificationReplay {
                        expired: true,
                        current_cursor,
                        next_cursor: after_cursor,
                        more_events: false,
                        events: Vec::new(),
                    });
                }
                let rows = sqlx::query(
                    r#"
                SELECT
                    cursor,
                    mailbox_id,
                    NULLIF(summary_json->>'messageId', '')::uuid AS message_id,
                    change_kind,
                    modseq,
                    to_char(
                        created_at AT TIME ZONE 'UTC',
                        'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'
                    ) AS created_at
                FROM mail_change_log
                WHERE tenant_id = $1
                  AND (account_id = $2 OR affected_principal_ids @> ARRAY[$2]::uuid[])
                  AND mailbox_id = $3
                  AND object_kind = 'mailbox_message'
                  AND NOT COALESCE((summary_json->>'mapiOnly')::boolean, FALSE)
                  AND cursor > $4
                  AND ($5::bigint IS NULL OR cursor <= $5)
                  AND (retained_until IS NULL OR retained_until > NOW())
                  AND change_kind IN ('created', 'moved', 'updated', 'destroyed', 'expunged')
                ORDER BY cursor ASC
                LIMIT $6
                "#,
                )
                .bind(tenant_id)
                .bind(account_id)
                .bind(mailbox_id)
                .bind(after_cursor)
                .bind(current_cursor)
                .bind((limit.max(1) + 1) as i64)
                .fetch_all(&mut *tx)
                .await?;
                tx.commit().await?;
                let more_events = rows.len() > limit.max(1);
                let events = rows
                    .into_iter()
                    .take(limit.max(1))
                    .map(|row| {
                        Ok(EwsNotificationLogEvent {
                            cursor: row.try_get("cursor")?,
                            mailbox_id: row.try_get("mailbox_id")?,
                            message_id: row.try_get("message_id")?,
                            change_kind: row.try_get("change_kind")?,
                            modseq: row.try_get::<i64, _>("modseq")?.max(0) as u64,
                            created_at: row.try_get("created_at")?,
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;
                let next_cursor = events
                    .last()
                    .map(|event| event.cursor)
                    .or(current_cursor)
                    .unwrap_or(after_cursor);
                Ok(EwsNotificationReplay {
                    expired: false,
                    current_cursor,
                    next_cursor,
                    more_events,
                    events,
                })
            })
        }
    };
}
