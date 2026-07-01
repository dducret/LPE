macro_rules! store_impl_messages {
    () => {
    fn create_jmap_mailbox<'a>(
        &'a self,
        input: JmapMailboxCreateInput,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, JmapMailbox> {
        Box::pin(async move { self.create_jmap_mailbox(input, audit).await })
    }

    fn update_jmap_mailbox<'a>(
        &'a self,
        input: JmapMailboxUpdateInput,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, JmapMailbox> {
        Box::pin(async move { self.update_jmap_mailbox(input, audit).await })
    }

    fn destroy_jmap_mailbox<'a>(
        &'a self,
        account_id: Uuid,
        mailbox_id: Uuid,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, ()> {
        Box::pin(async move {
            self.destroy_jmap_mailbox(account_id, mailbox_id, audit)
                .await
        })
    }

    fn query_jmap_email_ids<'a>(
        &'a self,
        account_id: Uuid,
        mailbox_id: Option<Uuid>,
        search_text: Option<&'a str>,
        position: u64,
        limit: u64,
    ) -> StoreFuture<'a, JmapEmailQuery> {
        Box::pin(async move {
            self.query_jmap_email_ids(account_id, mailbox_id, search_text, position, limit)
                .await
        })
    }

    fn query_mapi_content_table_ids<'a>(
        &'a self,
        account_id: Uuid,
        query: MapiContentTableQuery,
    ) -> StoreFuture<'a, MapiContentTableQueryResult> {
        Box::pin(async move {
            let tenant_id = sqlx::query_scalar::<_, Uuid>(
                r#"
                SELECT tenant_id
                FROM accounts
                WHERE id = $1
                LIMIT 1
                "#,
            )
            .bind(account_id)
            .fetch_optional(self.pool())
            .await?
            .ok_or_else(|| anyhow::anyhow!("account not found"))?;

            let total = sqlx::query_scalar::<_, i64>(
                r#"
                SELECT COUNT(DISTINCT mm.message_id)
                FROM mailbox_messages mm
                WHERE mm.tenant_id = $1
                  AND mm.account_id = $2
                  AND mm.mailbox_id = $3
                  AND mm.visibility = 'visible'
                "#,
            )
            .bind(tenant_id)
            .bind(account_id)
            .bind(query.mailbox_id)
            .fetch_one(self.pool())
            .await?;

            let order_by = mapi_content_table_order_by(&query.sort_orders);
            let sql = format!(
                r#"
                WITH row_source AS (
                    SELECT
                        m.id,
                        m.received_at,
                        lower(COALESCE(m.normalized_subject, '')) AS subject_key,
                        lower(COALESCE(fr.display_name, fr.address, '')) AS sender_name_key,
                        lower(COALESCE(fr.address, '')) AS sender_email_key,
                        lower(COALESCE(to_rollup.display_to, '')) AS display_to_key,
                        m.size_octets,
                        m.has_attachments,
                        ((CASE WHEN mm.is_seen THEN 1 ELSE 0 END)
                            + (CASE WHEN m.has_attachments THEN 16 ELSE 0 END)) AS message_flags
                    FROM mailbox_messages mm
                    JOIN messages m
                      ON m.tenant_id = mm.tenant_id
                     AND m.id = mm.message_id
                    LEFT JOIN message_recipients fr
                      ON fr.tenant_id = m.tenant_id
                     AND fr.message_id = m.id
                     AND fr.role = 'from'
                    LEFT JOIN LATERAL (
                        SELECT string_agg(COALESCE(NULLIF(r.display_name, ''), r.address), '; ' ORDER BY r.ordinal) AS display_to
                        FROM message_recipients r
                        WHERE r.tenant_id = m.tenant_id
                          AND r.message_id = m.id
                          AND r.role = 'to'
                    ) to_rollup ON TRUE
                    WHERE mm.tenant_id = $1
                      AND mm.account_id = $2
                      AND mm.mailbox_id = $3
                      AND mm.visibility = 'visible'
                )
                SELECT id
                FROM row_source
                ORDER BY {order_by}
                OFFSET $4
                LIMIT $5
                "#
            );
            let ids = sqlx::query(&sql)
                .bind(tenant_id)
                .bind(account_id)
                .bind(query.mailbox_id)
                .bind(query.position as i64)
                .bind(query.limit as i64)
                .fetch_all(self.pool())
                .await?
                .into_iter()
                .map(|row| row.try_get("id"))
                .collect::<std::result::Result<Vec<Uuid>, sqlx::Error>>()?;

            Ok(MapiContentTableQueryResult {
                ids,
                total: total.max(0) as u64,
            })
        })
    }

    fn list_recoverable_items<'a>(
        &'a self,
        account_id: Uuid,
        recoverable_folder: Option<&'a str>,
    ) -> StoreFuture<'a, Vec<RecoverableItem>> {
        Box::pin(async move {
            self.list_recoverable_items(account_id, recoverable_folder)
                .await
        })
    }

    fn restore_recoverable_item<'a>(
        &'a self,
        account_id: Uuid,
        recoverable_item_id: Uuid,
        target_mailbox_id: Option<Uuid>,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, JmapEmail> {
        Box::pin(async move {
            self.restore_recoverable_item(account_id, recoverable_item_id, target_mailbox_id, audit)
                .await
        })
    }

    fn purge_recoverable_item<'a>(
        &'a self,
        account_id: Uuid,
        recoverable_item_id: Uuid,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, ()> {
        Box::pin(async move {
            self.purge_recoverable_item(account_id, recoverable_item_id, audit)
                .await
        })
    }

    fn fetch_all_jmap_email_ids<'a>(&'a self, account_id: Uuid) -> StoreFuture<'a, Vec<Uuid>> {
        Box::pin(async move { self.fetch_all_jmap_email_ids(account_id).await })
    }

    fn fetch_jmap_emails<'a>(
        &'a self,
        account_id: Uuid,
        ids: &'a [Uuid],
    ) -> StoreFuture<'a, Vec<JmapEmail>> {
        Box::pin(async move { self.fetch_jmap_emails(account_id, ids).await })
    }

    fn fetch_jmap_emails_with_protected_bcc<'a>(
        &'a self,
        account_id: Uuid,
        ids: &'a [Uuid],
    ) -> StoreFuture<'a, Vec<JmapEmail>> {
        Box::pin(async move {
            self.fetch_jmap_emails_with_protected_bcc(account_id, ids)
                .await
        })
    }

    fn fetch_message_attachments<'a>(
        &'a self,
        account_id: Uuid,
        message_id: Uuid,
    ) -> StoreFuture<'a, Vec<ActiveSyncAttachment>> {
        Box::pin(async move {
            self.fetch_activesync_message_attachments(account_id, message_id)
                .await
        })
    }

    fn fetch_calendar_attachments_for_events<'a>(
        &'a self,
        account_id: Uuid,
        event_ids: &'a [Uuid],
    ) -> StoreFuture<'a, Vec<(Uuid, Vec<CalendarEventAttachment>)>> {
        Box::pin(async move {
            self.fetch_calendar_attachments_for_events(account_id, event_ids)
                .await
        })
    }

    fn fetch_attachment_content<'a>(
        &'a self,
        account_id: Uuid,
        file_reference: &'a str,
    ) -> StoreFuture<'a, Option<ActiveSyncAttachmentContent>> {
        Box::pin(async move {
            if let Some(content) = self
                .fetch_activesync_attachment_content(account_id, file_reference)
                .await?
            {
                return Ok(Some(content));
            }
            self.fetch_calendar_attachment_content(account_id, file_reference)
                .await
        })
    }

    fn add_message_attachment<'a>(
        &'a self,
        account_id: Uuid,
        message_id: Uuid,
        attachment: AttachmentUploadInput,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, Option<(JmapEmail, ActiveSyncAttachment)>> {
        Box::pin(async move {
            self.add_message_attachment(account_id, message_id, attachment, audit)
                .await
        })
    }

    fn add_calendar_event_attachment<'a>(
        &'a self,
        account_id: Uuid,
        event_id: Uuid,
        attachment: AttachmentUploadInput,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, Option<CalendarEventAttachment>> {
        Box::pin(async move {
            self.add_calendar_event_attachment(account_id, event_id, attachment, audit)
                .await
        })
    }

    fn delete_message_attachment<'a>(
        &'a self,
        account_id: Uuid,
        file_reference: &'a str,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, Option<JmapEmail>> {
        Box::pin(async move {
            self.delete_message_attachment(account_id, file_reference, audit)
                .await
        })
    }

    fn delete_calendar_event_attachment<'a>(
        &'a self,
        account_id: Uuid,
        file_reference: &'a str,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, Option<Uuid>> {
        Box::pin(async move {
            self.delete_calendar_event_attachment(account_id, file_reference, audit)
                .await
        })
    }

    fn import_jmap_email<'a>(
        &'a self,
        input: JmapImportedEmailInput,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, JmapEmail> {
        Box::pin(async move { self.import_jmap_email(input, audit).await })
    }

    fn move_jmap_email<'a>(
        &'a self,
        account_id: Uuid,
        message_id: Uuid,
        target_mailbox_id: Uuid,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, JmapEmail> {
        Box::pin(async move {
            self.move_jmap_email(account_id, message_id, target_mailbox_id, audit)
                .await
        })
    }

    fn move_jmap_email_from_mailbox<'a>(
        &'a self,
        account_id: Uuid,
        source_mailbox_id: Uuid,
        message_id: Uuid,
        target_mailbox_id: Uuid,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, JmapEmail> {
        Box::pin(async move {
            self.move_jmap_email_from_mailbox(
                account_id,
                source_mailbox_id,
                message_id,
                target_mailbox_id,
                audit,
            )
            .await
        })
    }

    fn copy_jmap_email<'a>(
        &'a self,
        account_id: Uuid,
        message_id: Uuid,
        target_mailbox_id: Uuid,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, JmapEmail> {
        Box::pin(async move {
            self.copy_jmap_email(account_id, message_id, target_mailbox_id, audit)
                .await
        })
    }

    fn update_jmap_email_flags<'a>(
        &'a self,
        account_id: Uuid,
        message_id: Uuid,
        unread: Option<bool>,
        flagged: Option<bool>,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, JmapEmail> {
        Box::pin(async move {
            lpe_storage::mail_items::update_message_flags(
                self,
                account_id,
                message_id,
                lpe_storage::mail_items::MessageFlagUpdate { unread, flagged },
                audit,
            )
            .await
        })
    }

    fn update_jmap_email_followup_flags<'a>(
        &'a self,
        account_id: Uuid,
        message_id: Uuid,
        update: JmapEmailFollowupUpdate,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, JmapEmail> {
        Box::pin(async move {
            self.update_jmap_email_followup_flags(account_id, message_id, update, audit)
                .await
        })
    }

    fn update_jmap_email_content<'a>(
        &'a self,
        account_id: Uuid,
        message_id: Uuid,
        subject: Option<String>,
        body_text: Option<String>,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, JmapEmail> {
        Box::pin(async move {
            self.update_jmap_email_content(account_id, message_id, subject, body_text, audit)
                .await
        })
    }

    fn delete_jmap_email<'a>(
        &'a self,
        account_id: Uuid,
        message_id: Uuid,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, ()> {
        Box::pin(async move { self.delete_jmap_email(account_id, message_id, audit).await })
    }

    fn delete_jmap_email_from_mailbox<'a>(
        &'a self,
        account_id: Uuid,
        mailbox_id: Uuid,
        message_id: Uuid,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, ()> {
        Box::pin(async move {
            self.delete_jmap_email_from_mailbox(account_id, mailbox_id, message_id, audit)
                .await
        })
    }

    fn replace_message_recipients<'a>(
        &'a self,
        account_id: Uuid,
        message_id: Uuid,
        to: &'a [SubmittedRecipientInput],
        cc: &'a [SubmittedRecipientInput],
        bcc: &'a [SubmittedRecipientInput],
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, ()> {
        Box::pin(async move {
            self.replace_message_recipients(account_id, message_id, to, cc, bcc, audit)
                .await
        })
    }

    fn save_draft_message<'a>(
        &'a self,
        input: SubmitMessageInput,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, SavedDraftMessage> {
        Box::pin(async move { self.save_draft_message(input, audit).await })
    }

    fn submit_draft_message<'a>(
        &'a self,
        account_id: Uuid,
        draft_message_id: Uuid,
        submitted_by_account_id: Uuid,
        source: &'a str,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, SubmittedMessage> {
        Box::pin(async move {
            self.submit_draft_message(
                account_id,
                draft_message_id,
                submitted_by_account_id,
                source,
                audit,
            )
            .await
        })
    }

    fn submit_message<'a>(
        &'a self,
        input: SubmitMessageInput,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, SubmittedMessage> {
        Box::pin(async move { self.submit_message(input, audit).await })
    }

    fn cancel_queued_submission<'a>(
        &'a self,
        account_id: Uuid,
        message_id: Uuid,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, CancelSubmissionResult> {
        Box::pin(async move {
            self.cancel_queued_submission(account_id, message_id, audit)
                .await
        })
    }
    };
}
