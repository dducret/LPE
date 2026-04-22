use anyhow::{bail, Result};
use sqlx::Row;
use uuid::Uuid;

use crate::{
    preview_text, submission, ActiveSyncSyncState, ActiveSyncSyncStateRow, AuditEntryInput,
    CanonicalChangeCategory, JmapEmail, JmapImportedEmailInput, SenderAuthorizationKind, Storage,
};

impl Storage {
    pub async fn delete_client_contact(&self, account_id: Uuid, contact_id: Uuid) -> Result<()> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let mut tx = self.pool.begin().await?;
        let deleted = sqlx::query(
            r#"
            DELETE FROM contacts
            WHERE tenant_id = $1 AND account_id = $2 AND id = $3
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
        let deleted = sqlx::query(
            r#"
            DELETE FROM calendar_events
            WHERE tenant_id = $1 AND account_id = $2 AND id = $3
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
        self.fetch_jmap_emails(account_id, &[message_id])
            .await?
            .into_iter()
            .next()
            .ok_or_else(|| anyhow::anyhow!("message not found"))?;

        let target_mailbox = sqlx::query(
            r#"
            SELECT role, display_name
            FROM mailboxes
            WHERE tenant_id = $1 AND account_id = $2 AND id = $3
            LIMIT 1
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(target_mailbox_id)
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| anyhow::anyhow!("target mailbox not found"))?;

        let target_role = target_mailbox.try_get::<String, _>("role")?;
        let copied_message_id = Uuid::new_v4();
        let delivery_status = if target_role == "drafts" {
            "draft"
        } else {
            "stored"
        };

        let mut tx = self.pool.begin().await?;
        let modseq = self
            .allocate_mail_modseq_in_tx(&mut tx, &tenant_id, account_id)
            .await?;
        sqlx::query(
            r#"
            INSERT INTO messages (
                id, tenant_id, account_id, mailbox_id, thread_id, internet_message_id,
                imap_modseq, received_at, sent_at, from_display, from_address, sender_display,
                sender_address, sender_authorization_kind, submitted_by_account_id, subject_normalized,
                preview_text, unread, flagged, has_attachments, size_octets, mime_blob_ref,
                submission_source, delivery_status
            )
            SELECT
                $4, tenant_id, account_id, $5, thread_id, internet_message_id,
                $6, NOW(),
                CASE WHEN $7 = 'draft' THEN NULL ELSE sent_at END,
                from_display, from_address, sender_display,
                sender_address, sender_authorization_kind, submitted_by_account_id, subject_normalized,
                preview_text, unread, flagged, has_attachments, size_octets, mime_blob_ref,
                submission_source, $7
            FROM messages
            WHERE tenant_id = $1 AND account_id = $2 AND id = $3
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(message_id)
        .bind(copied_message_id)
        .bind(target_mailbox_id)
        .bind(modseq)
        .bind(delivery_status)
        .execute(&mut *tx)
        .await?;

        sqlx::query(
            r#"
            INSERT INTO message_bodies (
                message_id, body_text, body_html_sanitized, participants_normalized,
                language_code, content_hash, search_vector
            )
            SELECT
                $2, body_text, body_html_sanitized, participants_normalized,
                language_code, $3, search_vector
            FROM message_bodies
            WHERE message_id = $1
            "#,
        )
        .bind(message_id)
        .bind(copied_message_id)
        .bind(format!("copy:{copied_message_id}"))
        .execute(&mut *tx)
        .await?;

        let recipient_rows = sqlx::query(
            r#"
            SELECT kind, address, display_name, ordinal
            FROM message_recipients
            WHERE tenant_id = $1 AND message_id = $2
            ORDER BY kind ASC, ordinal ASC
            "#,
        )
        .bind(&tenant_id)
        .bind(message_id)
        .fetch_all(&mut *tx)
        .await?;
        for row in recipient_rows {
            sqlx::query(
                r#"
                INSERT INTO message_recipients (id, tenant_id, message_id, kind, address, display_name, ordinal)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                "#,
            )
            .bind(Uuid::new_v4())
            .bind(&tenant_id)
            .bind(copied_message_id)
            .bind(row.try_get::<String, _>("kind")?)
            .bind(row.try_get::<String, _>("address")?)
            .bind(row.try_get::<Option<String>, _>("display_name")?)
            .bind(row.try_get::<i32, _>("ordinal")?)
            .execute(&mut *tx)
            .await?;
        }

        let bcc_rows = sqlx::query(
            r#"
            SELECT address, display_name, ordinal, metadata_scope
            FROM message_bcc_recipients
            WHERE tenant_id = $1 AND message_id = $2
            ORDER BY ordinal ASC
            "#,
        )
        .bind(&tenant_id)
        .bind(message_id)
        .fetch_all(&mut *tx)
        .await?;
        for row in bcc_rows {
            sqlx::query(
                r#"
                INSERT INTO message_bcc_recipients (id, tenant_id, message_id, address, display_name, ordinal, metadata_scope)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                "#,
            )
            .bind(Uuid::new_v4())
            .bind(&tenant_id)
            .bind(copied_message_id)
            .bind(row.try_get::<String, _>("address")?)
            .bind(row.try_get::<Option<String>, _>("display_name")?)
            .bind(row.try_get::<i32, _>("ordinal")?)
            .bind(row.try_get::<String, _>("metadata_scope")?)
            .execute(&mut *tx)
            .await?;
        }

        let attachment_rows = sqlx::query(
            r#"
            SELECT file_name, media_type, size_octets, blob_ref, extracted_text, attachment_blob_id
            FROM attachments
            WHERE tenant_id = $1 AND message_id = $2
            ORDER BY file_name ASC
            "#,
        )
        .bind(&tenant_id)
        .bind(message_id)
        .fetch_all(&mut *tx)
        .await?;
        for row in attachment_rows {
            sqlx::query(
                r#"
                INSERT INTO attachments (
                    id, tenant_id, message_id, file_name, media_type, size_octets,
                    blob_ref, extracted_text, extracted_text_tsv, attachment_blob_id
                )
                VALUES (
                    $1, $2, $3, $4, $5, $6,
                    $7, $8, to_tsvector('simple', COALESCE($8, '')), $9
                )
                "#,
            )
            .bind(Uuid::new_v4())
            .bind(&tenant_id)
            .bind(copied_message_id)
            .bind(row.try_get::<String, _>("file_name")?)
            .bind(row.try_get::<String, _>("media_type")?)
            .bind(row.try_get::<i64, _>("size_octets")?)
            .bind(row.try_get::<String, _>("blob_ref")?)
            .bind(row.try_get::<Option<String>, _>("extracted_text")?)
            .bind(row.try_get::<Option<Uuid>, _>("attachment_blob_id")?)
            .execute(&mut *tx)
            .await?;
        }

        self.insert_audit(&mut tx, &tenant_id, audit).await?;
        tx.commit().await?;

        self.fetch_jmap_emails(account_id, &[copied_message_id])
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
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        self.fetch_jmap_emails(account_id, &[message_id])
            .await?
            .into_iter()
            .next()
            .ok_or_else(|| anyhow::anyhow!("message not found"))?;

        let target_mailbox_exists = sqlx::query_scalar::<_, i64>(
            r#"
            SELECT 1
            FROM mailboxes
            WHERE tenant_id = $1 AND account_id = $2 AND id = $3
            LIMIT 1
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(target_mailbox_id)
        .fetch_optional(&self.pool)
        .await?
        .is_some();
        if !target_mailbox_exists {
            bail!("target mailbox not found");
        }

        let mut tx = self.pool.begin().await?;
        let modseq = self
            .allocate_mail_modseq_in_tx(&mut tx, &tenant_id, account_id)
            .await?;
        let moved = sqlx::query(
            r#"
            UPDATE messages
            SET
                mailbox_id = $4,
                imap_uid = nextval('message_imap_uid_seq'),
                imap_modseq = $5
            WHERE tenant_id = $1 AND account_id = $2 AND id = $3
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(message_id)
        .bind(target_mailbox_id)
        .bind(modseq)
        .execute(&mut *tx)
        .await?;
        if moved.rows_affected() == 0 {
            bail!("message not found");
        }

        self.insert_audit(&mut tx, &tenant_id, audit).await?;
        Self::emit_mail_change(&mut tx, &tenant_id, account_id).await?;
        tx.commit().await?;

        self.fetch_jmap_emails(account_id, &[message_id])
            .await?
            .into_iter()
            .next()
            .ok_or_else(|| anyhow::anyhow!("moved message not found"))
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
        let preview = preview_text(&input.body_text);
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
        let delivery_status = if target_role == "drafts" {
            "draft"
        } else {
            "stored"
        };

        let mut tx = self.pool.begin().await?;
        let tenant_id = self.tenant_id_for_account_id(input.account_id).await?;
        self.ensure_account_exists(&mut tx, &tenant_id, input.account_id)
            .await?;
        let modseq = self
            .allocate_mail_modseq_in_tx(&mut tx, &tenant_id, input.account_id)
            .await?;
        sqlx::query(
            r#"
            INSERT INTO messages (
                id, tenant_id, account_id, mailbox_id, thread_id, internet_message_id,
                imap_modseq, received_at, sent_at, from_display, from_address, sender_display,
                sender_address, sender_authorization_kind, submitted_by_account_id, subject_normalized,
                preview_text, unread, flagged, has_attachments, size_octets, mime_blob_ref,
                submission_source, delivery_status
            )
            VALUES (
                $1, $2, $3, $4, $5, $6,
                $7, NOW(), NULL, $8, $9, $10,
                $11, $12, $13, $14, $15, FALSE, FALSE, FALSE, $16, $17,
                $18, $19
            )
            "#,
        )
        .bind(message_id)
        .bind(&tenant_id)
        .bind(input.account_id)
        .bind(input.mailbox_id)
        .bind(thread_id)
        .bind(input.internet_message_id)
        .bind(modseq)
        .bind(input.from_display)
        .bind(crate::normalize_email(&input.from_address))
        .bind(input.sender_display)
        .bind(input.sender_address.map(|value| crate::normalize_email(&value)))
        .bind(SenderAuthorizationKind::SelfSend.as_str())
        .bind(input.submitted_by_account_id)
        .bind(crate::normalize_subject(&input.subject))
        .bind(preview)
        .bind(input.size_octets.max(0))
        .bind(input.mime_blob_ref)
        .bind(input.source)
        .bind(delivery_status)
        .execute(&mut *tx)
        .await?;

        sqlx::query(
            r#"
            INSERT INTO message_bodies (
                message_id, body_text, body_html_sanitized, participants_normalized,
                language_code, content_hash, search_vector
            )
            VALUES ($1, $2, $3, $4, NULL, $5, to_tsvector('simple', $6))
            "#,
        )
        .bind(message_id)
        .bind(&input.body_text)
        .bind(input.body_html_sanitized)
        .bind(&participants)
        .bind(format!("import:{message_id}"))
        .bind(format!(
            "{} {} {}",
            crate::normalize_subject(&input.subject),
            input.body_text,
            participants
        ))
        .execute(&mut *tx)
        .await?;

        for (ordinal, recipient) in input.to.iter().enumerate() {
            sqlx::query(
                r#"
                INSERT INTO message_recipients (id, tenant_id, message_id, kind, address, display_name, ordinal)
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
                INSERT INTO message_recipients (id, tenant_id, message_id, kind, address, display_name, ordinal)
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
                INSERT INTO message_bcc_recipients (id, tenant_id, message_id, address, display_name, ordinal, metadata_scope)
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
        let row = sqlx::query_as::<_, ActiveSyncSyncStateRow>(
            r#"
            SELECT sync_key, snapshot_json
            FROM activesync_sync_states
            WHERE tenant_id = $1
              AND account_id = $2
              AND device_id = $3
              AND collection_id = $4
            ORDER BY created_at DESC
            LIMIT 1
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(device_id.trim())
        .bind(collection_id.trim())
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(|row| ActiveSyncSyncState {
            sync_key: row.sync_key,
            snapshot_json: row.snapshot_json,
        }))
    }
}
