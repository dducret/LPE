use anyhow::{bail, Result};
use sqlx::Row;
use uuid::Uuid;

use crate::{
    sha256_hex, submission, ActiveSyncSyncState, ActiveSyncSyncStateRow, AuditEntryInput,
    CanonicalChangeCategory, JmapEmail, JmapImportedEmailInput, Storage,
};

impl Storage {
    pub async fn delete_client_contact(&self, account_id: Uuid, contact_id: Uuid) -> Result<()> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let mut tx = self.pool.begin().await?;
        let exists = sqlx::query_scalar::<_, Uuid>(
            r#"
            SELECT id
            FROM contacts
            WHERE tenant_id = $1 AND owner_account_id = $2 AND id = $3
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(contact_id)
        .fetch_optional(&mut *tx)
        .await?;

        if exists.is_none() {
            bail!("contact not found");
        }

        self.insert_collaboration_tombstone_in_tx(
            &mut tx,
            &tenant_id,
            CanonicalChangeCategory::Contacts,
            account_id,
            None,
            "contact",
            contact_id,
            None,
            &[account_id],
        )
        .await?;
        let deleted = sqlx::query(
            r#"
            DELETE FROM contacts
            WHERE tenant_id = $1 AND owner_account_id = $2 AND id = $3
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(contact_id)
        .execute(&mut *tx)
        .await?;

        if deleted.rows_affected() == 0 {
            bail!("contact not found");
        }

        Self::emit_collaboration_change(
            &mut tx,
            &tenant_id,
            CanonicalChangeCategory::Contacts,
            account_id,
        )
        .await?;
        tx.commit().await?;

        Ok(())
    }

    pub async fn delete_client_event(&self, account_id: Uuid, event_id: Uuid) -> Result<()> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let mut tx = self.pool.begin().await?;
        let exists = sqlx::query_scalar::<_, Uuid>(
            r#"
            SELECT id
            FROM calendar_events
            WHERE tenant_id = $1 AND owner_account_id = $2 AND id = $3
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(event_id)
        .fetch_optional(&mut *tx)
        .await?;

        if exists.is_none() {
            bail!("event not found");
        }

        self.insert_collaboration_tombstone_in_tx(
            &mut tx,
            &tenant_id,
            CanonicalChangeCategory::Calendar,
            account_id,
            None,
            "calendar_event",
            event_id,
            None,
            &[account_id],
        )
        .await?;
        let deleted = sqlx::query(
            r#"
            DELETE FROM calendar_events
            WHERE tenant_id = $1 AND owner_account_id = $2 AND id = $3
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(event_id)
        .execute(&mut *tx)
        .await?;

        if deleted.rows_affected() == 0 {
            bail!("event not found");
        }

        Self::emit_collaboration_change(
            &mut tx,
            &tenant_id,
            CanonicalChangeCategory::Calendar,
            account_id,
        )
        .await?;
        tx.commit().await?;

        Ok(())
    }

    pub async fn copy_jmap_email(
        &self,
        account_id: Uuid,
        message_id: Uuid,
        target_mailbox_id: Uuid,
        audit: AuditEntryInput,
    ) -> Result<JmapEmail> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let mut tx = self.pool.begin().await?;
        let target_role = sqlx::query_scalar::<_, String>(
            r#"
            SELECT role
            FROM mailboxes
            WHERE tenant_id = $1 AND account_id = $2 AND id = $3
            LIMIT 1
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(target_mailbox_id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| anyhow::anyhow!("target mailbox not found"))?;
        let source = sqlx::query(
            r#"
            SELECT message_id, thread_id, is_seen, is_flagged, received_at::text AS received_at
            FROM mailbox_messages
            WHERE tenant_id = $1
              AND account_id = $2
              AND message_id = $3
              AND visibility = 'visible'
            ORDER BY updated_at DESC
            LIMIT 1
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(message_id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| anyhow::anyhow!("message not found"))?;
        if sqlx::query_scalar::<_, bool>(
            r#"
            SELECT EXISTS (
                SELECT 1
                FROM mailbox_messages
                WHERE tenant_id = $1
                  AND account_id = $2
                  AND mailbox_id = $3
                  AND message_id = $4
                  AND visibility <> 'expunged'
            )
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(target_mailbox_id)
        .bind(message_id)
        .fetch_one(&mut *tx)
        .await?
        {
            bail!("message already exists in target mailbox");
        }

        let membership_id = self
            .allocate_mailbox_membership_in_tx(
                &mut tx,
                &tenant_id,
                account_id,
                target_mailbox_id,
                message_id,
                source.try_get("thread_id")?,
                &source.try_get::<String, _>("received_at")?,
                source.try_get("is_seen")?,
                source.try_get("is_flagged")?,
                target_role == "drafts",
                "created",
            )
            .await?;
        sqlx::query(
            r#"
            INSERT INTO mail_search_documents (
                tenant_id, account_id, mailbox_message_id, message_id,
                subject_text, participants_visible, body_text, attachment_text, search_vector
            )
            SELECT
                tenant_id, account_id, $3, message_id,
                subject_text, participants_visible, body_text, attachment_text, search_vector
            FROM mail_search_documents
            WHERE tenant_id = $1 AND message_id = $2
            ORDER BY updated_at DESC
            LIMIT 1
            "#,
        )
        .bind(&tenant_id)
        .bind(message_id)
        .bind(membership_id)
        .execute(&mut *tx)
        .await?;
        self.insert_audit(&mut tx, &tenant_id, audit).await?;
        Self::emit_mail_change(&mut tx, &tenant_id, account_id).await?;
        tx.commit().await?;

        self.fetch_jmap_emails(account_id, &[message_id])
            .await?
            .into_iter()
            .next()
            .ok_or_else(|| anyhow::anyhow!("copied message not found"))
    }

    pub async fn move_jmap_email(
        &self,
        account_id: Uuid,
        message_id: Uuid,
        target_mailbox_id: Uuid,
        audit: AuditEntryInput,
    ) -> Result<JmapEmail> {
        self.move_jmap_email_membership(account_id, None, message_id, target_mailbox_id, audit)
            .await
    }

    pub async fn move_jmap_email_from_mailbox(
        &self,
        account_id: Uuid,
        source_mailbox_id: Uuid,
        message_id: Uuid,
        target_mailbox_id: Uuid,
        audit: AuditEntryInput,
    ) -> Result<JmapEmail> {
        self.move_jmap_email_membership(
            account_id,
            Some(source_mailbox_id),
            message_id,
            target_mailbox_id,
            audit,
        )
        .await
    }

    async fn move_jmap_email_membership(
        &self,
        account_id: Uuid,
        source_mailbox_id: Option<Uuid>,
        message_id: Uuid,
        target_mailbox_id: Uuid,
        audit: AuditEntryInput,
    ) -> Result<JmapEmail> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let mut tx = self.pool.begin().await?;
        let modseq = self
            .allocate_mail_modseq_in_tx(&mut tx, &tenant_id, account_id)
            .await?;
        let source = sqlx::query(
            r#"
            SELECT id, mailbox_id, thread_id, imap_uid, is_seen, is_flagged, received_at::text AS received_at
            FROM mailbox_messages
            WHERE tenant_id = $1
              AND account_id = $2
              AND message_id = $3
              AND ($4::uuid IS NULL OR mailbox_id = $4)
              AND visibility = 'visible'
            ORDER BY updated_at DESC
            LIMIT 1
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(message_id)
        .bind(source_mailbox_id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| anyhow::anyhow!("message not found"))?;
        let source_mailbox_id: Uuid = source.try_get("mailbox_id")?;
        if source_mailbox_id == target_mailbox_id {
            tx.rollback().await?;
            return self
                .fetch_jmap_emails(account_id, &[message_id])
                .await?
                .into_iter()
                .next()
                .ok_or_else(|| anyhow::anyhow!("moved message not found"));
        }
        let target_role = sqlx::query_scalar::<_, String>(
            r#"
            SELECT role
            FROM mailboxes
            WHERE tenant_id = $1 AND account_id = $2 AND id = $3
            LIMIT 1
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(target_mailbox_id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| anyhow::anyhow!("target mailbox not found"))?;
        if sqlx::query_scalar::<_, bool>(
            r#"
            SELECT EXISTS (
                SELECT 1
                FROM mailbox_messages
                WHERE tenant_id = $1
                  AND account_id = $2
                  AND mailbox_id = $3
                  AND message_id = $4
                  AND visibility <> 'expunged'
            )
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(target_mailbox_id)
        .bind(message_id)
        .fetch_one(&mut *tx)
        .await?
        {
            bail!("message already exists in target mailbox");
        }
        let target_uid: i64 = sqlx::query_scalar(
            r#"
            UPDATE mailboxes
            SET uid_next = uid_next + 1,
                total_messages = total_messages + 1,
                unread_messages = unread_messages + CASE WHEN $4 THEN 0 ELSE 1 END,
                modseq = GREATEST(modseq + 1, $5),
                updated_at = NOW()
            WHERE tenant_id = $1 AND account_id = $2 AND id = $3
            RETURNING uid_next - 1
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(target_mailbox_id)
        .bind(source.try_get::<bool, _>("is_seen")?)
        .bind(modseq)
        .fetch_one(&mut *tx)
        .await?;
        let source_membership_id: Uuid = source.try_get("id")?;
        let source_imap_uid: i64 = source.try_get("imap_uid")?;
        let thread_id: Uuid = source.try_get("thread_id")?;
        let target_membership_id = Uuid::new_v4();
        sqlx::query(
            r#"
            INSERT INTO mailbox_messages (
                id, tenant_id, account_id, mailbox_id, message_id, thread_id,
                imap_uid, modseq, is_seen, is_flagged, is_draft, received_at
            )
            VALUES (
                $1, $2, $3, $4, $5, $6,
                $7, $8, $9, $10, $11, COALESCE($12::timestamptz, NOW())
            )
            "#,
        )
        .bind(target_membership_id)
        .bind(&tenant_id)
        .bind(account_id)
        .bind(target_mailbox_id)
        .bind(message_id)
        .bind(thread_id)
        .bind(target_uid)
        .bind(modseq)
        .bind(source.try_get::<bool, _>("is_seen")?)
        .bind(source.try_get::<bool, _>("is_flagged")?)
        .bind(target_role == "drafts")
        .bind(source.try_get::<String, _>("received_at")?)
        .execute(&mut *tx)
        .await?;
        sqlx::query(
            r#"
            UPDATE mailbox_messages
            SET visibility = 'expunged',
                expunged_at = NOW(),
                modseq = $4,
                updated_at = NOW()
            WHERE tenant_id = $1 AND account_id = $2 AND id = $3
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(source_membership_id)
        .bind(modseq)
        .execute(&mut *tx)
        .await?;
        sqlx::query(
            r#"
            UPDATE mailboxes
            SET total_messages = GREATEST(0, total_messages - 1),
                unread_messages = GREATEST(0, unread_messages - CASE WHEN $4 THEN 0 ELSE 1 END),
                modseq = GREATEST(modseq + 1, $5),
                updated_at = NOW()
            WHERE tenant_id = $1 AND account_id = $2 AND id = $3
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(source_mailbox_id)
        .bind(source.try_get::<bool, _>("is_seen")?)
        .bind(modseq)
        .execute(&mut *tx)
        .await?;
        sqlx::query(
            r#"
            INSERT INTO mail_search_documents (
                tenant_id, account_id, mailbox_message_id, message_id,
                subject_text, participants_visible, body_text, attachment_text, search_vector
            )
            SELECT
                tenant_id, account_id, $3, message_id,
                subject_text, participants_visible, body_text, attachment_text, search_vector
            FROM mail_search_documents
            WHERE tenant_id = $1 AND message_id = $2
            ORDER BY updated_at DESC
            LIMIT 1
            "#,
        )
        .bind(&tenant_id)
        .bind(message_id)
        .bind(target_membership_id)
        .execute(&mut *tx)
        .await?;
        sqlx::query(
            r#"
            DELETE FROM mail_search_documents
            WHERE tenant_id = $1 AND account_id = $2 AND mailbox_message_id = $3
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(source_membership_id)
        .execute(&mut *tx)
        .await?;

        self.insert_audit(&mut tx, &tenant_id, audit).await?;
        let principals =
            Self::affected_mail_principals_in_tx(&mut tx, &tenant_id, account_id).await?;
        Self::insert_mail_change_log_in_tx(
            &mut tx,
            &tenant_id,
            Some(account_id),
            Some(target_mailbox_id),
            "mailbox_message",
            target_membership_id,
            "moved",
            modseq,
            &principals,
            serde_json::json!({
                "messageId": message_id,
                "sourceMailboxId": source_mailbox_id,
                "targetMailboxId": target_mailbox_id,
                "sourceMailboxMessageId": source_membership_id,
                "targetMailboxMessageId": target_membership_id,
                "threadId": thread_id,
                "imapUid": target_uid,
                "sourceImapUid": source_imap_uid,
                "targetImapUid": target_uid
            }),
        )
        .await?;
        let source_cursor = Self::insert_mail_change_log_in_tx(
            &mut tx,
            &tenant_id,
            Some(account_id),
            Some(source_mailbox_id),
            "mailbox_message",
            source_membership_id,
            "updated",
            modseq,
            &principals,
            serde_json::json!({
                "messageId": message_id,
                "threadId": thread_id,
                "imapUid": source_imap_uid,
                "targetMailboxId": target_mailbox_id,
                "sourceMailboxMessageId": source_membership_id,
                "targetMailboxMessageId": target_membership_id,
                "sourceImapUid": source_imap_uid,
                "targetImapUid": target_uid
            }),
        )
        .await?;
        sqlx::query(
            r#"
            INSERT INTO tombstones (
                id, tenant_id, account_id, mailbox_id, object_kind, object_id,
                message_id, mailbox_message_id, imap_uid, deleted_modseq,
                change_cursor, reason
            )
            VALUES ($1, $2, $3, $4, 'mailbox_message', $5, $6, $5, $7, $8, $9, 'move')
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(&tenant_id)
        .bind(account_id)
        .bind(source_mailbox_id)
        .bind(source_membership_id)
        .bind(message_id)
        .bind(source_imap_uid)
        .bind(modseq)
        .bind(source_cursor)
        .execute(&mut *tx)
        .await?;
        Self::emit_mail_change(&mut tx, &tenant_id, account_id).await?;
        tx.commit().await?;

        self.fetch_jmap_emails(account_id, &[message_id])
            .await?
            .into_iter()
            .next()
            .ok_or_else(|| anyhow::anyhow!("moved message not found"))
    }

    pub async fn update_jmap_email_flags(
        &self,
        account_id: Uuid,
        message_id: Uuid,
        unread: Option<bool>,
        flagged: Option<bool>,
        audit: AuditEntryInput,
    ) -> Result<JmapEmail> {
        self.update_jmap_email_followup_flags(
            account_id,
            message_id,
            crate::JmapEmailFollowupUpdate {
                unread,
                flagged,
                followup_flag_status: flagged.map(|flagged| {
                    if flagged {
                        "flagged".to_string()
                    } else {
                        "none".to_string()
                    }
                }),
                ..Default::default()
            },
            audit,
        )
        .await
    }

    pub async fn update_jmap_email_followup_flags(
        &self,
        account_id: Uuid,
        message_id: Uuid,
        update: crate::JmapEmailFollowupUpdate,
        audit: AuditEntryInput,
    ) -> Result<JmapEmail> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        if update.unread.is_none()
            && update.flagged.is_none()
            && update.followup_flag_status.is_none()
            && update.followup_icon.is_none()
            && update.todo_item_flags.is_none()
            && update.followup_request.is_none()
            && update.followup_start_at.is_none()
            && update.followup_due_at.is_none()
            && update.followup_completed_at.is_none()
            && update.reminder_set.is_none()
            && update.reminder_at.is_none()
            && update.reminder_dismissed_at.is_none()
            && update.swapped_todo_store_id.is_none()
            && update.swapped_todo_data.is_none()
        {
            return self
                .fetch_jmap_emails(account_id, &[message_id])
                .await?
                .into_iter()
                .next()
                .ok_or_else(|| anyhow::anyhow!("message not found"));
        }
        if let Some(status) = update.followup_flag_status.as_deref() {
            if !matches!(status, "none" | "flagged" | "complete") {
                bail!("invalid follow-up flag status");
            }
        }
        if update.followup_icon.is_some_and(|value| value < 0)
            || update.todo_item_flags.is_some_and(|value| value < 0)
        {
            bail!("invalid follow-up flag value");
        }

        let mut tx = self.pool.begin().await?;
        let modseq = self
            .allocate_mail_modseq_in_tx(&mut tx, &tenant_id, account_id)
            .await?;
        let rows = sqlx::query(
            r#"
            UPDATE mailbox_messages
            SET is_seen = CASE WHEN $4::bool IS NULL THEN is_seen ELSE NOT $4 END,
                is_flagged = CASE
                    WHEN $5::bool IS NOT NULL THEN $5
                    WHEN $6::text IS NULL THEN is_flagged
                    ELSE $6 IN ('flagged', 'complete')
                END,
                followup_flag_status = COALESCE($6, followup_flag_status),
                followup_icon = CASE
                    WHEN $6 = 'none' THEN 0
                    WHEN $7::integer IS NOT NULL THEN $7
                    WHEN $6 = 'flagged' AND followup_icon = 0 THEN 6
                    ELSE followup_icon
                END,
                todo_item_flags = CASE
                    WHEN $6 = 'none' THEN 0
                    WHEN $8::integer IS NOT NULL THEN $8
                    WHEN $6 IN ('flagged', 'complete') AND todo_item_flags = 0 THEN 8
                    ELSE todo_item_flags
                END,
                followup_request = COALESCE($9, followup_request),
                followup_start_at = CASE
                    WHEN $6 = 'none' THEN NULL
                    WHEN $10::text = '' THEN NULL
                    WHEN $10::text IS NOT NULL THEN $10::timestamptz
                    ELSE followup_start_at
                END,
                followup_due_at = CASE
                    WHEN $6 = 'none' THEN NULL
                    WHEN $11::text = '' THEN NULL
                    WHEN $11::text IS NOT NULL THEN $11::timestamptz
                    ELSE followup_due_at
                END,
                followup_completed_at = CASE
                    WHEN $6 IN ('none', 'flagged') THEN NULL
                    WHEN $12::text IS NOT NULL THEN $12::timestamptz
                    WHEN $6 = 'complete' THEN COALESCE(followup_completed_at, NOW())
                    ELSE followup_completed_at
                END,
                reminder_set = CASE
                    WHEN $6 = 'none' THEN FALSE
                    WHEN $13::bool IS NOT NULL THEN $13
                    ELSE reminder_set
                END,
                reminder_at = CASE
                    WHEN $6 = 'none' THEN NULL
                    WHEN $14::text = '' THEN NULL
                    WHEN $14::text IS NOT NULL THEN $14::timestamptz
                    ELSE reminder_at
                END,
                reminder_dismissed_at = CASE
                    WHEN $6 = 'none' THEN NULL
                    WHEN $15::text = '' THEN NULL
                    WHEN $15::text IS NOT NULL THEN $15::timestamptz
                    ELSE reminder_dismissed_at
                END,
                swapped_todo_store_id = COALESCE($16, swapped_todo_store_id),
                swapped_todo_data = COALESCE($17, swapped_todo_data),
                modseq = $18,
                updated_at = NOW()
            WHERE tenant_id = $1 AND account_id = $2 AND message_id = $3
              AND visibility = 'visible'
            RETURNING id, mailbox_id, thread_id, imap_uid
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(message_id)
        .bind(update.unread)
        .bind(update.flagged)
        .bind(update.followup_flag_status)
        .bind(update.followup_icon)
        .bind(update.todo_item_flags)
        .bind(update.followup_request)
        .bind(update.followup_start_at)
        .bind(update.followup_due_at)
        .bind(update.followup_completed_at)
        .bind(update.reminder_set)
        .bind(update.reminder_at)
        .bind(update.reminder_dismissed_at)
        .bind(update.swapped_todo_store_id)
        .bind(update.swapped_todo_data)
        .bind(modseq)
        .fetch_all(&mut *tx)
        .await?;
        if rows.is_empty() {
            bail!("message not found");
        }

        self.insert_audit(&mut tx, &tenant_id, audit).await?;
        let principals =
            Self::affected_mail_principals_in_tx(&mut tx, &tenant_id, account_id).await?;
        for row in rows {
            Self::insert_mail_change_log_in_tx(
                &mut tx,
                &tenant_id,
                Some(account_id),
                Some(row.try_get("mailbox_id")?),
                "mailbox_message",
                row.try_get("id")?,
                "updated",
                modseq,
                &principals,
                serde_json::json!({
                    "messageId": message_id,
                    "threadId": row.try_get::<Uuid, _>("thread_id")?,
                    "imapUid": row.try_get::<i64, _>("imap_uid")?
                }),
            )
            .await?;
        }
        Self::emit_mail_change(&mut tx, &tenant_id, account_id).await?;
        tx.commit().await?;

        self.fetch_jmap_emails(account_id, &[message_id])
            .await?
            .into_iter()
            .next()
            .ok_or_else(|| anyhow::anyhow!("updated message not found"))
    }

    pub async fn import_jmap_email(
        &self,
        input: JmapImportedEmailInput,
        audit: AuditEntryInput,
    ) -> Result<JmapEmail> {
        let tenant_id = self.tenant_id_for_account_id(input.account_id).await?;
        let target_mailbox = sqlx::query(
            r#"
            SELECT role
            FROM mailboxes
            WHERE tenant_id = $1 AND account_id = $2 AND id = $3
            LIMIT 1
            "#,
        )
        .bind(&tenant_id)
        .bind(input.account_id)
        .bind(input.mailbox_id)
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| anyhow::anyhow!("target mailbox not found"))?;
        let target_role = target_mailbox.try_get::<String, _>("role")?;

        let message_id = Uuid::new_v4();
        let thread_id = Uuid::new_v4();
        let recipients = input
            .to
            .iter()
            .cloned()
            .map(|recipient| ("to", recipient))
            .chain(input.cc.iter().cloned().map(|recipient| ("cc", recipient)))
            .collect::<Vec<_>>();
        let participants = submission::participants_normalized(
            &crate::normalize_email(&input.from_address),
            &recipients,
        );

        let mut tx = self.pool.begin().await?;
        let tenant_id = self.tenant_id_for_account_id(input.account_id).await?;
        self.ensure_account_exists(&mut tx, &tenant_id, input.account_id)
            .await?;
        let domain_id = self
            .load_account_domain_id_in_tx(&mut tx, &tenant_id, input.account_id)
            .await?;
        let raw_message = input.raw_message.clone().unwrap_or_else(|| {
            format!(
                "From: {}\r\nSubject: {}\r\n\r\n{}",
                crate::normalize_email(&input.from_address),
                input.subject,
                input.body_text
            )
            .into_bytes()
        });
        let blob_id = self
            .store_message_blob_in_tx(
                &mut tx,
                &tenant_id,
                domain_id,
                "raw_message",
                "message/rfc822",
                &raw_message,
            )
            .await?;
        sqlx::query(
            r#"
            INSERT INTO messages (
                id, tenant_id, domain_id, blob_id, internet_message_id, message_hash,
                normalized_subject, sent_at, received_at, size_octets, has_attachments
            )
            VALUES (
                $1, $2, $3, $4, $5, $6,
                $7, NULL, NOW(), $8, FALSE
            )
            "#,
        )
        .bind(message_id)
        .bind(&tenant_id)
        .bind(domain_id)
        .bind(blob_id)
        .bind(input.internet_message_id)
        .bind(sha256_hex(&raw_message))
        .bind(crate::normalize_subject(&input.subject))
        .bind(input.size_octets.max(0))
        .execute(&mut *tx)
        .await?;

        self.replace_message_headers_in_tx(&mut tx, &tenant_id, message_id, &raw_message)
            .await?;

        self.upsert_message_body_in_tx(
            &mut tx,
            &tenant_id,
            domain_id,
            message_id,
            &input.body_text,
            input.body_html_sanitized.as_deref(),
        )
        .await?;

        sqlx::query(
            r#"
            INSERT INTO message_recipients (id, tenant_id, message_id, role, address, display_name, ordinal)
            VALUES ($1, $2, $3, 'from', $4, $5, 0)
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(&tenant_id)
        .bind(message_id)
        .bind(crate::normalize_email(&input.from_address))
        .bind(input.from_display.as_deref())
        .execute(&mut *tx)
        .await?;

        for (ordinal, recipient) in input.to.iter().enumerate() {
            sqlx::query(
                r#"
                INSERT INTO message_recipients (id, tenant_id, message_id, role, address, display_name, ordinal)
                VALUES ($1, $2, $3, 'to', $4, $5, $6)
                "#,
            )
            .bind(Uuid::new_v4())
            .bind(&tenant_id)
            .bind(message_id)
            .bind(&recipient.address)
            .bind(recipient.display_name.as_deref())
            .bind(ordinal as i32)
            .execute(&mut *tx)
            .await?;
        }
        for (ordinal, recipient) in input.cc.iter().enumerate() {
            sqlx::query(
                r#"
                INSERT INTO message_recipients (id, tenant_id, message_id, role, address, display_name, ordinal)
                VALUES ($1, $2, $3, 'cc', $4, $5, $6)
                "#,
            )
            .bind(Uuid::new_v4())
            .bind(&tenant_id)
            .bind(message_id)
            .bind(&recipient.address)
            .bind(recipient.display_name.as_deref())
            .bind(ordinal as i32)
            .execute(&mut *tx)
            .await?;
        }
        for (ordinal, recipient) in input.bcc.iter().enumerate() {
            sqlx::query(
                r#"
                INSERT INTO protected_bcc_recipients (id, tenant_id, message_id, address, display_name, ordinal, metadata_scope)
                VALUES ($1, $2, $3, $4, $5, $6, 'audit-compliance')
                "#,
            )
            .bind(Uuid::new_v4())
            .bind(&tenant_id)
            .bind(message_id)
            .bind(&recipient.address)
            .bind(recipient.display_name.as_deref())
            .bind(ordinal as i32)
            .execute(&mut *tx)
            .await?;
        }

        self.ingest_message_attachments_in_tx(
            &mut tx,
            &tenant_id,
            input.account_id,
            message_id,
            &input.attachments,
        )
        .await?;
        let membership_id = self
            .allocate_mailbox_membership_in_tx(
                &mut tx,
                &tenant_id,
                input.account_id,
                input.mailbox_id,
                message_id,
                thread_id,
                "",
                true,
                false,
                target_role == "drafts",
                "created",
            )
            .await?;
        self.assign_message_attachments_membership_in_tx(
            &mut tx,
            &tenant_id,
            input.account_id,
            message_id,
            membership_id,
        )
        .await?;
        Self::upsert_mail_search_document_in_tx(
            &mut tx,
            &tenant_id,
            input.account_id,
            membership_id,
            message_id,
            &crate::normalize_subject(&input.subject),
            &participants,
            &input.body_text,
            "",
        )
        .await?;

        self.insert_audit(&mut tx, &tenant_id, audit).await?;
        Self::emit_mail_change(&mut tx, &tenant_id, input.account_id).await?;
        tx.commit().await?;

        self.fetch_jmap_emails(input.account_id, &[message_id])
            .await?
            .into_iter()
            .next()
            .ok_or_else(|| anyhow::anyhow!("imported message not found"))
    }

    pub async fn fetch_latest_activesync_sync_state(
        &self,
        account_id: Uuid,
        device_id: &str,
        collection_id: &str,
    ) -> Result<Option<ActiveSyncSyncState>> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let collection_kind = crate::protocols::activesync_collection_kind(collection_id);
        let row = sqlx::query_as::<_, ActiveSyncSyncStateRow>(
            r#"
            SELECT sync_key, state_json::text AS snapshot_json
            FROM activesync_sync_cursors
            WHERE tenant_id = $1
              AND account_id = $2
              AND device_id = $3
              AND collection_kind = $4
              AND collection_key = $5
              AND expires_at > NOW()
            ORDER BY created_at DESC
            LIMIT 1
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(device_id.trim())
        .bind(collection_kind)
        .bind(collection_id.trim())
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(|row| ActiveSyncSyncState {
            sync_key: row.sync_key,
            snapshot_json: row.snapshot_json,
        }))
    }
}
