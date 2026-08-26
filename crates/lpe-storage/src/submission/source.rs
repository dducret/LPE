use anyhow::{anyhow, bail, Result};
use sqlx::{Postgres, Row};
use uuid::Uuid;

use super::{
    insert_visible_recipient, normalize_bcc_recipients, normalize_visible_recipients,
    types::ResolvedSubmissionAuthorization, AttachmentUploadInput, SubmitMessageInput,
    SubmittedRecipientInput,
};
use crate::{
    blob_store::{DurableBlobKind, PostgresBlobStore},
    normalize_subject, sha256_hex, Storage,
};

#[derive(Debug, Clone)]
pub(super) struct ClaimedSubmissionSource {
    pub(super) membership_id: Uuid,
    pub(super) account_id: Uuid,
    pub(super) mailbox_id: Uuid,
    pub(super) message_id: Uuid,
    pub(super) thread_id: Option<Uuid>,
    pub(super) imap_uid: i64,
    pub(super) modseq: u64,
    pub(super) is_seen: bool,
    pub(super) internet_message_id: Option<String>,
    pub(super) date_header: Option<String>,
}

impl Storage {
    pub(super) async fn claim_submission_source_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, Postgres>,
        tenant_id: &Uuid,
        account_id: Uuid,
        message_id: Uuid,
        allow_outbox: bool,
    ) -> Result<ClaimedSubmissionSource> {
        self.lock_visible_message_memberships_in_tx(tx, tenant_id, account_id, message_id)
            .await?;
        let rows = sqlx::query(
            r#"
            SELECT mm.id, mm.mailbox_id, mm.message_id, mm.thread_id,
                   mm.imap_uid, mm.modseq, mm.is_seen,
                   m.internet_message_id,
                   date_header.header_value AS date_header
            FROM mailbox_messages mm
            JOIN mailboxes mb
              ON mb.tenant_id = mm.tenant_id
             AND mb.account_id = mm.account_id
             AND mb.id = mm.mailbox_id
            JOIN messages m
              ON m.tenant_id = mm.tenant_id
             AND m.id = mm.message_id
            LEFT JOIN LATERAL (
                SELECT header_value
                FROM message_headers
                WHERE tenant_id = m.tenant_id
                  AND message_id = m.id
                  AND lower(header_name) = 'date'
                ORDER BY ordinal, id
                LIMIT 1
            ) date_header ON TRUE
            WHERE mm.tenant_id = $1
              AND mm.account_id = $2
              AND mm.message_id = $3
              AND mm.visibility = 'visible'
              AND (mb.role = 'drafts' OR ($4 AND mb.role = 'outbox'))
            ORDER BY CASE WHEN mb.role = 'drafts' THEN 0 ELSE 1 END, mm.id
            FOR UPDATE OF mm
            "#,
        )
        .bind(tenant_id)
        .bind(account_id)
        .bind(message_id)
        .bind(allow_outbox)
        .fetch_all(&mut **tx)
        .await?;

        let missing = if allow_outbox {
            "submission source not found"
        } else {
            "draft not found"
        };
        if rows.is_empty() {
            bail!(missing);
        }
        if rows.len() != 1 {
            bail!("submission source is ambiguous");
        }
        let row = &rows[0];
        let modseq = row.try_get::<i64, _>("modseq")?;
        Ok(ClaimedSubmissionSource {
            membership_id: row.try_get("id")?,
            account_id,
            mailbox_id: row.try_get("mailbox_id")?,
            message_id: row.try_get("message_id")?,
            thread_id: row.try_get("thread_id")?,
            imap_uid: row.try_get("imap_uid")?,
            modseq: modseq
                .try_into()
                .map_err(|_| anyhow!("claimed submission source has an invalid modseq"))?,
            is_seen: row.try_get("is_seen")?,
            internet_message_id: row.try_get("internet_message_id")?,
            date_header: row.try_get("date_header")?,
        })
    }

    pub(super) async fn load_claimed_submission_source_input_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, Postgres>,
        tenant_id: &Uuid,
        account_id: Uuid,
        submitted_by_account_id: Uuid,
        source: &str,
        claim: &ClaimedSubmissionSource,
    ) -> Result<SubmitMessageInput> {
        let row = sqlx::query(
            r#"
            SELECT
                m.normalized_subject AS subject,
                m.internet_message_id,
                m.size_octets,
                mm.is_seen,
                mm.is_flagged,
                COALESCE(fr.address, '') AS from_address,
                NULLIF(fr.display_name, '') AS from_display,
                NULLIF(sr.address, '') AS sender_address,
                NULLIF(sr.display_name, '') AS sender_display,
                COALESCE(tb.body_text, '') AS body_text,
                hb.sanitized_html AS body_html_sanitized
            FROM mailbox_messages mm
            JOIN messages m
              ON m.tenant_id = mm.tenant_id
             AND m.id = mm.message_id
            LEFT JOIN LATERAL (
                SELECT address, display_name
                FROM message_recipients
                WHERE tenant_id = m.tenant_id AND message_id = m.id AND role = 'from'
                ORDER BY ordinal, id
                LIMIT 1
            ) fr ON TRUE
            LEFT JOIN LATERAL (
                SELECT address, display_name
                FROM message_recipients
                WHERE tenant_id = m.tenant_id AND message_id = m.id AND role = 'sender'
                ORDER BY ordinal, id
                LIMIT 1
            ) sr ON TRUE
            LEFT JOIN LATERAL (
                SELECT body_text
                FROM message_bodies
                WHERE tenant_id = m.tenant_id AND message_id = m.id AND body_kind = 'text'
                ORDER BY id
                LIMIT 1
            ) tb ON TRUE
            LEFT JOIN LATERAL (
                SELECT sanitized_html
                FROM message_bodies
                WHERE tenant_id = m.tenant_id AND message_id = m.id AND body_kind = 'html'
                ORDER BY id
                LIMIT 1
            ) hb ON TRUE
            WHERE mm.tenant_id = $1
              AND mm.account_id = $2
              AND mm.id = $3
              AND mm.message_id = $4
              AND mm.visibility = 'visible'
            "#,
        )
        .bind(tenant_id)
        .bind(account_id)
        .bind(claim.membership_id)
        .bind(claim.message_id)
        .fetch_optional(&mut **tx)
        .await?
        .ok_or_else(|| anyhow!("claimed submission source is no longer visible"))?;

        let recipient_rows = sqlx::query(
            r#"
            SELECT role, address, display_name
            FROM message_recipients
            WHERE tenant_id = $1
              AND message_id = $2
              AND role IN ('to', 'cc')
            ORDER BY role, ordinal, id
            "#,
        )
        .bind(tenant_id)
        .bind(claim.message_id)
        .fetch_all(&mut **tx)
        .await?;
        let bcc_rows = sqlx::query(
            r#"
            SELECT address, display_name
            FROM protected_bcc_recipients
            WHERE tenant_id = $1
              AND owner_account_id = $2
              AND message_id = $3
            ORDER BY ordinal, id
            "#,
        )
        .bind(tenant_id)
        .bind(account_id)
        .bind(claim.message_id)
        .fetch_all(&mut **tx)
        .await?;

        let mut to = Vec::new();
        let mut cc = Vec::new();
        for recipient in recipient_rows {
            let value = SubmittedRecipientInput {
                address: recipient.try_get("address")?,
                display_name: recipient.try_get("display_name")?,
            };
            match recipient.try_get::<String, _>("role")?.as_str() {
                "to" => to.push(value),
                "cc" => cc.push(value),
                _ => unreachable!("submission source query restricts recipient roles"),
            }
        }
        let bcc = bcc_rows
            .into_iter()
            .map(|recipient| {
                Ok(SubmittedRecipientInput {
                    address: recipient.try_get("address")?,
                    display_name: recipient.try_get("display_name")?,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let attachments = self
            .fetch_claimed_submission_source_attachment_inputs_in_tx(
                tx, tenant_id, account_id, claim,
            )
            .await?;

        Ok(SubmitMessageInput {
            draft_message_id: Some(claim.message_id),
            account_id,
            submitted_by_account_id,
            source: source.trim().to_lowercase(),
            from_display: row.try_get("from_display")?,
            from_address: row.try_get("from_address")?,
            sender_display: row.try_get("sender_display")?,
            sender_address: row.try_get("sender_address")?,
            to,
            cc,
            bcc,
            subject: row.try_get("subject")?,
            body_text: row.try_get("body_text")?,
            body_html_sanitized: row.try_get("body_html_sanitized")?,
            internet_message_id: row.try_get("internet_message_id")?,
            mime_blob_ref: Some(format!("message:{}", claim.message_id)),
            size_octets: row.try_get("size_octets")?,
            unread: Some(!row.try_get::<bool, _>("is_seen")?),
            flagged: Some(row.try_get("is_flagged")?),
            replace_attachments: false,
            attachments,
        })
    }

    pub(super) async fn replace_claimed_submission_source_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, Postgres>,
        tenant_id: &Uuid,
        claim: &ClaimedSubmissionSource,
        input: &SubmitMessageInput,
        authorization: &ResolvedSubmissionAuthorization,
    ) -> Result<()> {
        let subject = normalize_subject(&input.subject);
        let body_text = input.body_text.trim().to_string();
        let visible_recipients = normalize_visible_recipients(input);
        let bcc_recipients = normalize_bcc_recipients(input);
        let domain_id = self
            .load_account_domain_id_in_tx(tx, tenant_id, input.account_id)
            .await?;
        let raw_message = format!(
            "From: {}\r\nSubject: {}\r\n\r\n{}",
            authorization.from_address, input.subject, body_text
        );
        let blob_id = self
            .store_message_blob_in_tx(
                tx,
                tenant_id,
                domain_id,
                "raw_message",
                "message/rfc822",
                raw_message.as_bytes(),
            )
            .await?;

        let updated = sqlx::query(
            r#"
            UPDATE messages
            SET internet_message_id = $5,
                blob_id = $6,
                message_hash = $7,
                normalized_subject = $8,
                received_at = NOW(),
                sent_at = NULL,
                size_octets = $9,
                has_attachments = CASE WHEN $10 THEN FALSE ELSE has_attachments END
            WHERE tenant_id = $1
              AND id = $2
              AND EXISTS (
                  SELECT 1
                  FROM mailbox_messages mm
                  WHERE mm.tenant_id = messages.tenant_id
                    AND mm.account_id = $3
                    AND mm.id = $4
                    AND mm.message_id = messages.id
                    AND mm.visibility = 'visible'
              )
            "#,
        )
        .bind(tenant_id)
        .bind(claim.message_id)
        .bind(input.account_id)
        .bind(claim.membership_id)
        .bind(input.internet_message_id.clone())
        .bind(blob_id)
        .bind(sha256_hex(raw_message.as_bytes()))
        .bind(&subject)
        .bind(input.size_octets.max(0))
        .bind(input.replace_attachments)
        .execute(&mut **tx)
        .await?;
        if updated.rows_affected() != 1 {
            bail!("claimed submission source could not be updated");
        }

        self.replace_message_headers_in_tx(tx, tenant_id, claim.message_id, raw_message.as_bytes())
            .await?;
        self.upsert_message_body_in_tx(
            tx,
            tenant_id,
            domain_id,
            claim.message_id,
            &body_text,
            input.body_html_sanitized.as_deref(),
        )
        .await?;
        sqlx::query("DELETE FROM message_recipients WHERE tenant_id = $1 AND message_id = $2")
            .bind(tenant_id)
            .bind(claim.message_id)
            .execute(&mut **tx)
            .await?;
        sqlx::query(
            "DELETE FROM protected_bcc_recipients WHERE tenant_id = $1 AND message_id = $2",
        )
        .bind(tenant_id)
        .bind(claim.message_id)
        .execute(&mut **tx)
        .await?;

        insert_visible_recipient(
            tx,
            tenant_id,
            claim.message_id,
            "from",
            0,
            &SubmittedRecipientInput {
                address: authorization.from_address.clone(),
                display_name: authorization.from_display.clone(),
            },
        )
        .await?;
        if let Some(sender_address) = authorization.sender_address.as_ref() {
            insert_visible_recipient(
                tx,
                tenant_id,
                claim.message_id,
                "sender",
                0,
                &SubmittedRecipientInput {
                    address: sender_address.clone(),
                    display_name: authorization.sender_display.clone(),
                },
            )
            .await?;
        }
        for (ordinal, (role, recipient)) in visible_recipients.iter().enumerate() {
            insert_visible_recipient(tx, tenant_id, claim.message_id, role, ordinal, recipient)
                .await?;
        }
        for (ordinal, recipient) in bcc_recipients.iter().enumerate() {
            sqlx::query(
                r#"
                INSERT INTO protected_bcc_recipients (
                    id, tenant_id, message_id, owner_account_id, address, display_name, ordinal,
                    metadata_scope
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, 'audit-compliance')
                "#,
            )
            .bind(Uuid::new_v4())
            .bind(tenant_id)
            .bind(claim.message_id)
            .bind(input.account_id)
            .bind(&recipient.address)
            .bind(recipient.display_name.as_deref())
            .bind(ordinal as i32)
            .execute(&mut **tx)
            .await?;
        }

        if input.replace_attachments {
            self.delete_claimed_submission_source_attachments_in_tx(tx, tenant_id, claim, None)
                .await?;
        }
        self.ingest_message_attachments_in_tx(
            tx,
            tenant_id,
            input.account_id,
            claim.message_id,
            &input.attachments,
        )
        .await?;
        self.assign_message_attachments_membership_in_tx(
            tx,
            tenant_id,
            input.account_id,
            claim.message_id,
            claim.membership_id,
        )
        .await?;

        let membership = sqlx::query(
            r#"
            UPDATE mailbox_messages
            SET is_seen = NOT $5,
                is_flagged = $6,
                is_draft = TRUE,
                updated_at = NOW()
            WHERE tenant_id = $1
              AND account_id = $2
              AND id = $3
              AND message_id = $4
              AND visibility = 'visible'
            "#,
        )
        .bind(tenant_id)
        .bind(input.account_id)
        .bind(claim.membership_id)
        .bind(claim.message_id)
        .bind(input.unread.unwrap_or(false))
        .bind(input.flagged.unwrap_or(false))
        .execute(&mut **tx)
        .await?;
        if membership.rows_affected() != 1 {
            bail!("claimed submission source could not be updated");
        }
        Ok(())
    }

    pub(super) async fn fetch_claimed_submission_source_attachment_inputs_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, Postgres>,
        tenant_id: &Uuid,
        account_id: Uuid,
        claim: &ClaimedSubmissionSource,
    ) -> Result<Vec<AttachmentUploadInput>> {
        let rows = sqlx::query(
            r#"
            SELECT a.file_name, a.disposition, a.content_id, a.domain_id, a.blob_id,
                   mp.content_type,
                   COALESCE(mp.is_scheduling_body, FALSE) AS is_scheduling_body
            FROM attachments a
            LEFT JOIN mime_parts mp
              ON mp.tenant_id = a.tenant_id
             AND mp.message_id = a.message_id
             AND mp.id = a.mime_part_id
            WHERE a.tenant_id = $1
              AND a.account_id = $2
              AND a.message_id = $3
              AND EXISTS (
                  SELECT 1
                  FROM mailbox_messages mm
                  WHERE mm.tenant_id = a.tenant_id
                    AND mm.account_id = a.account_id
                    AND mm.id = $4
                    AND mm.message_id = a.message_id
                    AND mm.visibility = 'visible'
              )
            ORDER BY a.ordinal, a.id
            "#,
        )
        .bind(tenant_id)
        .bind(account_id)
        .bind(claim.message_id)
        .bind(claim.membership_id)
        .fetch_all(&mut **tx)
        .await?;

        let blob_store = PostgresBlobStore;
        let mut attachments = Vec::with_capacity(rows.len());
        for row in rows {
            let file_name: String = row.try_get("file_name")?;
            let domain_id: Uuid = row.try_get("domain_id")?;
            let blob_id: Uuid = row.try_get("blob_id")?;
            let blob = blob_store
                .read_durable_blob_in_tx(
                    tx,
                    tenant_id,
                    domain_id,
                    DurableBlobKind::Attachment,
                    blob_id,
                )
                .await?
                .ok_or_else(|| anyhow!("submission source attachment blob is unavailable"))?;
            attachments.push(AttachmentUploadInput {
                file_name,
                media_type: row
                    .try_get::<Option<String>, _>("content_type")?
                    .unwrap_or(blob.media_type),
                disposition: row.try_get("disposition")?,
                content_id: row.try_get("content_id")?,
                is_scheduling_body: row.try_get("is_scheduling_body")?,
                blob_bytes: blob.bytes,
            });
        }
        Ok(attachments)
    }

    pub(super) async fn delete_submission_source_message_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, Postgres>,
        tenant_id: &Uuid,
        account_id: Uuid,
        claim: &ClaimedSubmissionSource,
    ) -> Result<()> {
        self.delete_claimed_source_message_in_tx(tx, tenant_id, account_id, claim)
            .await
    }

    pub(super) async fn delete_draft_message_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, Postgres>,
        tenant_id: &Uuid,
        account_id: Uuid,
        message_id: Uuid,
    ) -> Result<()> {
        let claim = self
            .claim_submission_source_in_tx(tx, tenant_id, account_id, message_id, false)
            .await?;
        self.delete_claimed_source_message_in_tx(tx, tenant_id, account_id, &claim)
            .await
    }

    async fn delete_claimed_source_message_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, Postgres>,
        tenant_id: &Uuid,
        account_id: Uuid,
        claim: &ClaimedSubmissionSource,
    ) -> Result<()> {
        let modseq = self
            .allocate_mail_modseq_in_tx(tx, tenant_id, account_id)
            .await?;
        let expunged = sqlx::query(
            r#"
            UPDATE mailbox_messages
            SET visibility = 'expunged',
                expunged_at = NOW(),
                modseq = $5,
                updated_at = NOW()
            WHERE tenant_id = $1
              AND account_id = $2
              AND id = $3
              AND message_id = $4
              AND visibility = 'visible'
            "#,
        )
        .bind(tenant_id)
        .bind(account_id)
        .bind(claim.membership_id)
        .bind(claim.message_id)
        .bind(modseq)
        .execute(&mut **tx)
        .await?;
        if expunged.rows_affected() != 1 {
            bail!("claimed submission source was not visible at expunge");
        }

        let principals = Self::affected_mail_principals_in_tx(tx, tenant_id, account_id).await?;
        let cursor = Self::insert_mail_change_log_in_tx(
            tx,
            tenant_id,
            Some(account_id),
            Some(claim.mailbox_id),
            "mailbox_message",
            claim.membership_id,
            "destroyed",
            modseq,
            &principals,
            serde_json::json!({
                "messageId": claim.message_id,
                "threadId": claim.thread_id,
                "imapUid": claim.imap_uid
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
            VALUES ($1, $2, $3, $4, 'mailbox_message', $5, $6, $5, $7, $8, $9, 'destroyed')
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(tenant_id)
        .bind(account_id)
        .bind(claim.mailbox_id)
        .bind(claim.membership_id)
        .bind(claim.message_id)
        .bind(claim.imap_uid)
        .bind(modseq)
        .bind(cursor)
        .execute(&mut **tx)
        .await?;
        sqlx::query(
            r#"
            DELETE FROM mail_search_documents
            WHERE tenant_id = $1 AND account_id = $2 AND mailbox_message_id = $3
            "#,
        )
        .bind(tenant_id)
        .bind(account_id)
        .bind(claim.membership_id)
        .execute(&mut **tx)
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
        .bind(tenant_id)
        .bind(account_id)
        .bind(claim.mailbox_id)
        .bind(claim.is_seen)
        .bind(modseq)
        .execute(&mut **tx)
        .await?;
        Self::recalculate_mailbox_counts_in_tx(tx, tenant_id, account_id, claim.mailbox_id, modseq)
            .await?;

        Ok(())
    }
}
