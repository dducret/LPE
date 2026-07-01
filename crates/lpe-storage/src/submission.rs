use anyhow::{anyhow, bail, Result};
use sqlx::{Postgres, Row};
use uuid::Uuid;

use crate::{
    normalize_email, normalize_subject, sha256_hex, trim_optional_text, AuditEntryInput,
    JmapEmailRecipientRow, Storage,
};

mod delegation;
mod types;

use types::{
    canonical_submission_phases, source_protocol_sql, submission_authorization_kind_sql,
    CanonicalSubmissionPhase, ResolvedSubmissionAuthorization,
};
pub(crate) use types::{
    normalize_bcc_recipients, normalize_visible_recipients, participants_normalized,
    push_recipients, sender_authorization_kind_from_str, sender_identity_id, AccountIdentity,
};
pub use types::{
    AttachmentUploadInput, CancelSubmissionResult, MailboxAccountAccess, MailboxDelegationGrant,
    MailboxDelegationGrantInput, MailboxDelegationOverview, MailboxFolderDelegationGrantInput,
    SavedDraftMessage, SenderAuthorizationKind, SenderDelegationGrant, SenderDelegationGrantInput,
    SenderDelegationRight, SenderIdentity, SubmissionAccountIdentity, SubmitMessageInput,
    SubmittedMessage, SubmittedRecipientInput,
};

async fn insert_visible_recipient(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    tenant_id: &Uuid,
    message_id: Uuid,
    role: &str,
    ordinal: usize,
    recipient: &SubmittedRecipientInput,
) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO message_recipients (
            id, tenant_id, message_id, role, address, display_name, ordinal
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        "#,
    )
    .bind(Uuid::new_v4())
    .bind(tenant_id)
    .bind(message_id)
    .bind(role)
    .bind(&recipient.address)
    .bind(recipient.display_name.as_deref())
    .bind(ordinal as i32)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

impl Storage {
    pub async fn replace_message_recipients(
        &self,
        account_id: Uuid,
        message_id: Uuid,
        to: &[SubmittedRecipientInput],
        cc: &[SubmittedRecipientInput],
        bcc: &[SubmittedRecipientInput],
        audit: AuditEntryInput,
    ) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        self.ensure_account_exists(&mut tx, &tenant_id, account_id)
            .await?;
        let exists = sqlx::query_scalar::<_, bool>(
            r#"
            SELECT EXISTS (
                SELECT 1
                FROM mailbox_messages
                WHERE tenant_id = $1
                  AND account_id = $2
                  AND message_id = $3
                  AND visibility = 'visible'
            )
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(message_id)
        .fetch_one(&mut *tx)
        .await?;
        if !exists {
            bail!("message not found");
        }

        sqlx::query("DELETE FROM message_recipients WHERE tenant_id = $1 AND message_id = $2")
            .bind(&tenant_id)
            .bind(message_id)
            .execute(&mut *tx)
            .await?;
        sqlx::query(
            "DELETE FROM protected_bcc_recipients WHERE tenant_id = $1 AND message_id = $2",
        )
        .bind(&tenant_id)
        .bind(message_id)
        .execute(&mut *tx)
        .await?;

        for (ordinal, recipient) in to.iter().enumerate() {
            insert_visible_recipient(&mut tx, &tenant_id, message_id, "to", ordinal, recipient)
                .await?;
        }
        for (ordinal, recipient) in cc.iter().enumerate() {
            insert_visible_recipient(&mut tx, &tenant_id, message_id, "cc", ordinal, recipient)
                .await?;
        }
        for (ordinal, recipient) in bcc.iter().enumerate() {
            sqlx::query(
                r#"
                INSERT INTO protected_bcc_recipients (
                    id, tenant_id, message_id, address, display_name, ordinal
                )
                VALUES ($1, $2, $3, $4, $5, $6)
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

        self.insert_audit(&mut tx, &tenant_id, audit).await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn save_draft_message(
        &self,
        input: SubmitMessageInput,
        audit: AuditEntryInput,
    ) -> Result<SavedDraftMessage> {
        let from_address = normalize_email(&input.from_address);
        let subject = normalize_subject(&input.subject);
        let body_text = input.body_text.trim().to_string();
        let visible_recipients = normalize_visible_recipients(&input);
        let bcc_recipients = normalize_bcc_recipients(&input);

        if from_address.is_empty() {
            bail!("from_address is required");
        }

        let mut tx = self.pool.begin().await?;
        let tenant_id = self.tenant_id_for_account_id(input.account_id).await?;
        self.ensure_account_exists(&mut tx, &tenant_id, input.account_id)
            .await?;
        self.ensure_same_tenant_account_in_tx(&mut tx, &tenant_id, input.submitted_by_account_id)
            .await?;
        let authorization = self
            .resolve_submission_authorization_in_tx(&mut tx, &tenant_id, &input)
            .await?;
        let draft_mailbox_id = self
            .ensure_mailbox(
                &mut tx,
                &tenant_id,
                input.account_id,
                "drafts",
                "Drafts",
                10,
                365,
            )
            .await?;

        let message_id = input.draft_message_id.unwrap_or_else(Uuid::new_v4);
        let thread_id = Uuid::new_v4();
        let participants_normalized = participants_normalized(&from_address, &visible_recipients);
        let unread = input.unread.unwrap_or(false);
        let flagged = input.flagged.unwrap_or(false);
        let modseq = self
            .allocate_mail_modseq_in_tx(&mut tx, &tenant_id, input.account_id)
            .await?;
        let domain_id = self
            .load_account_domain_id_in_tx(&mut tx, &tenant_id, input.account_id)
            .await?;
        let raw_message = format!(
            "From: {}\r\nSubject: {}\r\n\r\n{}",
            authorization.from_address, input.subject, body_text
        );
        let blob_id = self
            .store_message_blob_in_tx(
                &mut tx,
                &tenant_id,
                domain_id,
                "raw_message",
                "message/rfc822",
                raw_message.as_bytes(),
            )
            .await?;

        if input.draft_message_id.is_some() {
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
                    has_attachments = FALSE
                WHERE tenant_id = $1
                  AND id = $3
                  AND EXISTS (
                      SELECT 1
                      FROM mailbox_messages mm
                      JOIN mailboxes mb
                        ON mb.tenant_id = mm.tenant_id
                       AND mb.account_id = mm.account_id
                       AND mb.id = mm.mailbox_id
                      WHERE mm.tenant_id = messages.tenant_id
                        AND mm.account_id = $2
                        AND mm.message_id = messages.id
                        AND mm.mailbox_id = $4
                        AND mm.visibility = 'visible'
                        AND mb.role = 'drafts'
                  )
                "#,
            )
            .bind(&tenant_id)
            .bind(input.account_id)
            .bind(message_id)
            .bind(draft_mailbox_id)
            .bind(input.internet_message_id)
            .bind(blob_id)
            .bind(sha256_hex(raw_message.as_bytes()))
            .bind(&subject)
            .bind(input.size_octets.max(0))
            .execute(&mut *tx)
            .await?;

            if updated.rows_affected() == 0 {
                bail!("draft not found");
            }

            self.replace_message_headers_in_tx(
                &mut tx,
                &tenant_id,
                message_id,
                raw_message.as_bytes(),
            )
            .await?;
            sqlx::query("DELETE FROM message_recipients WHERE tenant_id = $1 AND message_id = $2")
                .bind(&tenant_id)
                .bind(message_id)
                .execute(&mut *tx)
                .await?;
            sqlx::query(
                "DELETE FROM protected_bcc_recipients WHERE tenant_id = $1 AND message_id = $2",
            )
            .bind(&tenant_id)
            .bind(message_id)
            .execute(&mut *tx)
            .await?;
            sqlx::query("DELETE FROM attachments WHERE tenant_id = $1 AND message_id = $2")
                .bind(&tenant_id)
                .bind(message_id)
                .execute(&mut *tx)
                .await?;
            sqlx::query(
                r#"
                UPDATE mailbox_messages
                SET modseq = $4,
                    is_seen = NOT $5,
                    is_flagged = $6,
                    is_draft = TRUE,
                    updated_at = NOW()
                WHERE tenant_id = $1
                  AND account_id = $2
                  AND message_id = $3
                  AND mailbox_id = $7
                  AND visibility = 'visible'
                "#,
            )
            .bind(&tenant_id)
            .bind(input.account_id)
            .bind(message_id)
            .bind(modseq)
            .bind(unread)
            .bind(flagged)
            .bind(draft_mailbox_id)
            .execute(&mut *tx)
            .await?;
            Self::recalculate_mailbox_counts_in_tx(
                &mut tx,
                &tenant_id,
                input.account_id,
                draft_mailbox_id,
                modseq,
            )
            .await?;
        } else {
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
            .bind(sha256_hex(raw_message.as_bytes()))
            .bind(&subject)
            .bind(input.size_octets.max(0))
            .execute(&mut *tx)
            .await?;
            self.replace_message_headers_in_tx(
                &mut tx,
                &tenant_id,
                message_id,
                raw_message.as_bytes(),
            )
            .await?;
        }

        self.upsert_message_body_in_tx(
            &mut tx,
            &tenant_id,
            domain_id,
            message_id,
            &body_text,
            input.body_html_sanitized.as_deref(),
        )
        .await?;

        sqlx::query(
            r#"
            INSERT INTO message_recipients (
                id, tenant_id, message_id, role, address, display_name, ordinal
            )
            VALUES ($1, $2, $3, 'from', $4, $5, 0)
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(&tenant_id)
        .bind(message_id)
        .bind(&authorization.from_address)
        .bind(authorization.from_display.as_deref())
        .execute(&mut *tx)
        .await?;
        if let Some(sender_address) = authorization.sender_address.as_deref() {
            sqlx::query(
                r#"
                INSERT INTO message_recipients (
                    id, tenant_id, message_id, role, address, display_name, ordinal
                )
                VALUES ($1, $2, $3, 'sender', $4, $5, 0)
                "#,
            )
            .bind(Uuid::new_v4())
            .bind(&tenant_id)
            .bind(message_id)
            .bind(sender_address)
            .bind(authorization.sender_display.as_deref())
            .execute(&mut *tx)
            .await?;
        }

        for (ordinal, (kind, recipient)) in visible_recipients.iter().enumerate() {
            sqlx::query(
                r#"
                INSERT INTO message_recipients (
                    id, tenant_id, message_id, role, address, display_name, ordinal
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                "#,
            )
            .bind(Uuid::new_v4())
            .bind(&tenant_id)
            .bind(message_id)
            .bind(kind)
            .bind(&recipient.address)
            .bind(recipient.display_name.as_deref())
            .bind(ordinal as i32)
            .execute(&mut *tx)
            .await?;
        }

        for (ordinal, recipient) in bcc_recipients.iter().enumerate() {
            sqlx::query(
                r#"
                INSERT INTO protected_bcc_recipients (
                    id, tenant_id, message_id, address, display_name, ordinal, metadata_scope
                )
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
        let (membership_id, membership_thread_id, membership_imap_uid, existing_draft_update) =
            if input.draft_message_id.is_some() {
                let row = sqlx::query(
                    r#"
                    SELECT id, thread_id, imap_uid
                    FROM mailbox_messages
                    WHERE tenant_id = $1
                      AND account_id = $2
                      AND mailbox_id = $3
                      AND message_id = $4
                      AND visibility = 'visible'
                    LIMIT 1
                    "#,
                )
                .bind(&tenant_id)
                .bind(input.account_id)
                .bind(draft_mailbox_id)
                .bind(message_id)
                .fetch_one(&mut *tx)
                .await?;
                (
                    row.try_get::<Uuid, _>("id")?,
                    row.try_get::<Uuid, _>("thread_id")?,
                    row.try_get::<i64, _>("imap_uid")?,
                    true,
                )
            } else {
                let membership_id = self
                    .allocate_mailbox_membership_in_tx(
                        &mut tx,
                        &tenant_id,
                        input.account_id,
                        draft_mailbox_id,
                        message_id,
                        thread_id,
                        "",
                        !unread,
                        flagged,
                        true,
                        "created",
                    )
                    .await?;
                (membership_id, thread_id, 0, false)
            };
        if existing_draft_update {
            let principals =
                Self::affected_mail_principals_in_tx(&mut tx, &tenant_id, input.account_id).await?;
            Self::insert_mail_change_log_in_tx(
                &mut tx,
                &tenant_id,
                Some(input.account_id),
                Some(draft_mailbox_id),
                "mailbox_message",
                membership_id,
                "updated",
                modseq,
                &principals,
                serde_json::json!({
                    "messageId": message_id,
                    "threadId": membership_thread_id,
                    "imapUid": membership_imap_uid
                }),
            )
            .await?;
        }
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
            &subject,
            &participants_normalized,
            &body_text,
            "",
        )
        .await?;

        self.insert_audit(&mut tx, &tenant_id, audit).await?;
        Self::emit_mail_change(&mut tx, &tenant_id, input.account_id).await?;
        tx.commit().await?;

        Ok(SavedDraftMessage {
            message_id,
            account_id: input.account_id,
            submitted_by_account_id: input.submitted_by_account_id,
            draft_mailbox_id,
            delivery_status: "draft".to_string(),
        })
    }

    pub async fn submit_message(
        &self,
        input: SubmitMessageInput,
        audit: AuditEntryInput,
    ) -> Result<SubmittedMessage> {
        let subject = normalize_subject(&input.subject);
        let body_text = input.body_text.trim().to_string();
        let visible_recipients = normalize_visible_recipients(&input);
        let bcc_recipients = normalize_bcc_recipients(&input);

        if visible_recipients.is_empty() && bcc_recipients.is_empty() {
            bail!("at least one recipient is required");
        }
        if subject.is_empty() && body_text.is_empty() {
            bail!("subject or body_text is required");
        }

        let mut tx = self.pool.begin().await?;
        let tenant_id = self.tenant_id_for_account_id(input.account_id).await?;

        let account_exists = sqlx::query(
            r#"
            SELECT 1
            FROM accounts
            WHERE tenant_id = $1 AND id = $2
            LIMIT 1
            "#,
        )
        .bind(&tenant_id)
        .bind(input.account_id)
        .fetch_optional(&mut *tx)
        .await?;

        if account_exists.is_none() {
            bail!("account not found");
        }

        let authorization = self
            .resolve_submission_authorization_in_tx(&mut tx, &tenant_id, &input)
            .await?;

        let message_id = Uuid::new_v4();
        let thread_id = Uuid::new_v4();
        let outbound_queue_id = Uuid::new_v4();
        let participants_normalized =
            participants_normalized(&authorization.from_address, &visible_recipients);
        let domain_id = self
            .load_account_domain_id_in_tx(&mut tx, &tenant_id, input.account_id)
            .await?;
        let raw_message = format!(
            "From: {}\r\nSubject: {}\r\n\r\n{}",
            authorization.from_address, input.subject, body_text
        );
        let blob_id = self
            .store_message_blob_in_tx(
                &mut tx,
                &tenant_id,
                domain_id,
                "raw_message",
                "message/rfc822",
                raw_message.as_bytes(),
            )
            .await?;
        let mut sent_mailbox_id = None;
        let mut sent_mailbox_message_id = None;

        for phase in canonical_submission_phases(input.draft_message_id.is_some()) {
            match phase {
                CanonicalSubmissionPhase::EnsureSentMailbox => {
                    sent_mailbox_id = Some(
                        self.ensure_mailbox(
                            &mut tx,
                            &tenant_id,
                            input.account_id,
                            "sent",
                            "Sent",
                            20,
                            365,
                        )
                        .await?,
                    );
                }
                CanonicalSubmissionPhase::PersistSentMessage => {
                    let sent_mailbox_id = sent_mailbox_id
                        .ok_or_else(|| anyhow!("sent mailbox must exist before submission"))?;
                    sqlx::query(
                        r#"
                        INSERT INTO messages (
                            id, tenant_id, domain_id, blob_id, internet_message_id, message_hash,
                            normalized_subject, sent_at, received_at, size_octets, has_attachments
                        )
                        VALUES (
                            $1, $2, $3, $4, $5, $6,
                            $7, NOW(), NOW(), $8, FALSE
                        )
                        "#,
                    )
                    .bind(message_id)
                    .bind(&tenant_id)
                    .bind(domain_id)
                    .bind(blob_id)
                    .bind(input.internet_message_id.clone())
                    .bind(sha256_hex(raw_message.as_bytes()))
                    .bind(&subject)
                    .bind(input.size_octets.max(0))
                    .execute(&mut *tx)
                    .await?;

                    self.replace_message_headers_in_tx(
                        &mut tx,
                        &tenant_id,
                        message_id,
                        raw_message.as_bytes(),
                    )
                    .await?;
                    self.upsert_message_body_in_tx(
                        &mut tx,
                        &tenant_id,
                        domain_id,
                        message_id,
                        &body_text,
                        input.body_html_sanitized.as_deref(),
                    )
                    .await?;

                    sqlx::query(
                        r#"
                        INSERT INTO message_recipients (
                            id, tenant_id, message_id, role, address, display_name, ordinal
                        )
                        VALUES ($1, $2, $3, 'from', $4, $5, 0)
                        "#,
                    )
                    .bind(Uuid::new_v4())
                    .bind(&tenant_id)
                    .bind(message_id)
                    .bind(&authorization.from_address)
                    .bind(authorization.from_display.as_deref())
                    .execute(&mut *tx)
                    .await?;
                    if let Some(sender_address) = authorization.sender_address.as_deref() {
                        sqlx::query(
                            r#"
                            INSERT INTO message_recipients (
                                id, tenant_id, message_id, role, address, display_name, ordinal
                            )
                            VALUES ($1, $2, $3, 'sender', $4, $5, 0)
                            "#,
                        )
                        .bind(Uuid::new_v4())
                        .bind(&tenant_id)
                        .bind(message_id)
                        .bind(sender_address)
                        .bind(authorization.sender_display.as_deref())
                        .execute(&mut *tx)
                        .await?;
                    }

                    for (ordinal, (kind, recipient)) in visible_recipients.iter().enumerate() {
                        sqlx::query(
                            r#"
                            INSERT INTO message_recipients (
                                id, tenant_id, message_id, role, address, display_name, ordinal
                            )
                            VALUES ($1, $2, $3, $4, $5, $6, $7)
                            "#,
                        )
                        .bind(Uuid::new_v4())
                        .bind(&tenant_id)
                        .bind(message_id)
                        .bind(kind)
                        .bind(&recipient.address)
                        .bind(recipient.display_name.as_deref())
                        .bind(ordinal as i32)
                        .execute(&mut *tx)
                        .await?;
                    }

                    for (ordinal, recipient) in bcc_recipients.iter().enumerate() {
                        sqlx::query(
                            r#"
                            INSERT INTO protected_bcc_recipients (
                                id, tenant_id, message_id, address, display_name, ordinal, metadata_scope
                            )
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
                    let mailbox_message_id = self
                        .allocate_mailbox_membership_in_tx(
                            &mut tx,
                            &tenant_id,
                            input.account_id,
                            sent_mailbox_id,
                            message_id,
                            thread_id,
                            "",
                            true,
                            false,
                            false,
                            "created",
                        )
                        .await?;
                    self.assign_message_attachments_membership_in_tx(
                        &mut tx,
                        &tenant_id,
                        input.account_id,
                        message_id,
                        mailbox_message_id,
                    )
                    .await?;
                    Self::upsert_mail_search_document_in_tx(
                        &mut tx,
                        &tenant_id,
                        input.account_id,
                        mailbox_message_id,
                        message_id,
                        &subject,
                        &participants_normalized,
                        &body_text,
                        "",
                    )
                    .await?;
                    sent_mailbox_message_id = Some(mailbox_message_id);
                }
                CanonicalSubmissionPhase::PersistOutboundQueue => {
                    let sent_mailbox_message_id = sent_mailbox_message_id.ok_or_else(|| {
                        anyhow!("sent mailbox message must exist before queue handoff")
                    })?;
                    sqlx::query(
                        r#"
                        INSERT INTO submission_queue (
                            id, tenant_id, account_id, sent_mailbox_message_id,
                            from_address, sender_address, authorization_kind,
                            source_protocol, transport, status
                        )
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'lpe-ct-smtp', 'queued')
                        "#,
                    )
                    .bind(outbound_queue_id)
                    .bind(&tenant_id)
                    .bind(input.account_id)
                    .bind(sent_mailbox_message_id)
                    .bind(&authorization.from_address)
                    .bind(authorization.sender_address.as_deref())
                    .bind(submission_authorization_kind_sql(
                        authorization.authorization_kind,
                    ))
                    .bind(source_protocol_sql(&input.source))
                    .execute(&mut *tx)
                    .await?;
                    for (ordinal, (kind, recipient)) in visible_recipients.iter().enumerate() {
                        sqlx::query(
                            r#"
                            INSERT INTO submission_recipients (
                                id, tenant_id, submission_queue_id, role,
                                address, display_name, ordinal, protected_metadata
                            )
                            VALUES ($1, $2, $3, $4, $5, $6, $7, FALSE)
                            "#,
                        )
                        .bind(Uuid::new_v4())
                        .bind(&tenant_id)
                        .bind(outbound_queue_id)
                        .bind(kind)
                        .bind(&recipient.address)
                        .bind(recipient.display_name.as_deref())
                        .bind(ordinal as i32)
                        .execute(&mut *tx)
                        .await?;
                    }
                    for (ordinal, recipient) in bcc_recipients.iter().enumerate() {
                        sqlx::query(
                            r#"
                            INSERT INTO submission_recipients (
                                id, tenant_id, submission_queue_id, role,
                                address, display_name, ordinal, protected_metadata
                            )
                            VALUES ($1, $2, $3, 'bcc', $4, $5, $6, TRUE)
                            "#,
                        )
                        .bind(Uuid::new_v4())
                        .bind(&tenant_id)
                        .bind(outbound_queue_id)
                        .bind(&recipient.address)
                        .bind(recipient.display_name.as_deref())
                        .bind(ordinal as i32)
                        .execute(&mut *tx)
                        .await?;
                    }
                    let modseq = self
                        .allocate_mail_modseq_in_tx(&mut tx, &tenant_id, input.account_id)
                        .await?;
                    let principals =
                        Self::affected_mail_principals_in_tx(&mut tx, &tenant_id, input.account_id)
                            .await?;
                    Self::insert_mail_change_log_in_tx(
                        &mut tx,
                        &tenant_id,
                        Some(input.account_id),
                        None,
                        "submission",
                        outbound_queue_id,
                        "created",
                        modseq,
                        &principals,
                        serde_json::json!({
                            "messageId": message_id,
                            "status": "queued"
                        }),
                    )
                    .await?;
                }
                CanonicalSubmissionPhase::DeleteSourceDraft => {
                    if let Some(draft_message_id) = input.draft_message_id {
                        self.delete_draft_message_in_tx(
                            &mut tx,
                            &tenant_id,
                            input.account_id,
                            draft_message_id,
                        )
                        .await?;
                    }
                }
            }
        }

        self.insert_audit(&mut tx, &tenant_id, audit).await?;
        Self::emit_mail_change(&mut tx, &tenant_id, input.account_id).await?;
        tx.commit().await?;

        let sent_mailbox_id =
            sent_mailbox_id.ok_or_else(|| anyhow!("sent mailbox must exist after submission"))?;
        Ok(SubmittedMessage {
            message_id,
            thread_id,
            account_id: input.account_id,
            submitted_by_account_id: authorization.submitted_by.id,
            sent_mailbox_id,
            outbound_queue_id,
            delivery_status: "queued".to_string(),
        })
    }

    pub async fn submit_draft_message(
        &self,
        account_id: Uuid,
        draft_message_id: Uuid,
        submitted_by_account_id: Uuid,
        source: &str,
        audit: AuditEntryInput,
    ) -> Result<SubmittedMessage> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let draft = self
            .fetch_jmap_draft(account_id, draft_message_id)
            .await?
            .ok_or_else(|| anyhow!("draft not found"))?;

        let recipient_rows = sqlx::query_as::<_, JmapEmailRecipientRow>(
            r#"
            SELECT
                r.message_id,
                r.role AS kind,
                r.address,
                r.display_name,
                r.ordinal AS _ordinal
            FROM message_recipients r
            WHERE r.tenant_id = $1
              AND r.message_id = $2
            ORDER BY r.role ASC, r.ordinal ASC
            "#,
        )
        .bind(&tenant_id)
        .bind(draft_message_id)
        .fetch_all(&self.pool)
        .await?;

        let bcc_rows = sqlx::query(
            r#"
            SELECT address, display_name
            FROM protected_bcc_recipients
            WHERE tenant_id = $1 AND message_id = $2
            ORDER BY ordinal ASC
            "#,
        )
        .bind(&tenant_id)
        .bind(draft_message_id)
        .fetch_all(&self.pool)
        .await?;

        let to = recipient_rows
            .iter()
            .filter(|recipient| recipient.kind == "to")
            .map(|recipient| SubmittedRecipientInput {
                address: recipient.address.clone(),
                display_name: recipient.display_name.clone(),
            })
            .collect();
        let cc = recipient_rows
            .iter()
            .filter(|recipient| recipient.kind == "cc")
            .map(|recipient| SubmittedRecipientInput {
                address: recipient.address.clone(),
                display_name: recipient.display_name.clone(),
            })
            .collect();
        let bcc = bcc_rows
            .into_iter()
            .map(|row| SubmittedRecipientInput {
                address: row.try_get("address").unwrap_or_default(),
                display_name: row.try_get("display_name").ok(),
            })
            .collect();

        self.submit_message(
            SubmitMessageInput {
                draft_message_id: Some(draft_message_id),
                account_id,
                submitted_by_account_id,
                source: source.trim().to_lowercase(),
                from_display: draft.from_display,
                from_address: draft.from_address,
                sender_display: draft.sender_display,
                sender_address: draft.sender_address,
                to,
                cc,
                bcc,
                subject: draft.subject,
                body_text: draft.body_text,
                body_html_sanitized: draft.body_html_sanitized,
                internet_message_id: draft.internet_message_id,
                mime_blob_ref: None,
                size_octets: draft.size_octets,
                unread: Some(draft.unread),
                flagged: Some(draft.flagged),
                attachments: Vec::new(),
            },
            audit,
        )
        .await
    }

    pub async fn cancel_queued_submission(
        &self,
        account_id: Uuid,
        message_id: Uuid,
        audit: AuditEntryInput,
    ) -> Result<CancelSubmissionResult> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let mut tx = self.pool.begin().await?;
        let row = sqlx::query(
            r#"
            SELECT q.id, q.status
            FROM submission_queue q
            JOIN mailbox_messages mm
              ON mm.tenant_id = q.tenant_id
             AND mm.account_id = q.account_id
             AND mm.id = q.sent_mailbox_message_id
            JOIN mailboxes mb
              ON mb.tenant_id = mm.tenant_id
             AND mb.account_id = mm.account_id
             AND mb.id = mm.mailbox_id
            WHERE q.tenant_id = $1
              AND q.account_id = $2
              AND mm.message_id = $3
              AND mb.role = 'sent'
            ORDER BY q.created_at DESC
            LIMIT 1
            FOR UPDATE OF q
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(message_id)
        .fetch_optional(&mut *tx)
        .await?;

        let Some(row) = row else {
            return Ok(CancelSubmissionResult::NotFound);
        };
        let queue_id: Uuid = row.try_get("id")?;
        let status: String = row.try_get("status")?;

        if status == "cancelled" {
            return Ok(CancelSubmissionResult::AlreadyCancelled);
        }
        if !matches!(status.as_str(), "queued" | "ready" | "deferred") {
            return Ok(CancelSubmissionResult::NotCancellable);
        }

        sqlx::query(
            r#"
            UPDATE submission_queue
            SET status = 'cancelled',
                terminal_at = NOW(),
                updated_at = NOW()
            WHERE tenant_id = $1 AND id = $2
            "#,
        )
        .bind(&tenant_id)
        .bind(queue_id)
        .execute(&mut *tx)
        .await?;

        let trace_id = format!("mapi-abort-submit-{queue_id}");
        sqlx::query(
            r#"
            INSERT INTO submission_events (
                id, tenant_id, submission_queue_id, trace_id, event_kind, technical_json
            )
            VALUES (
                $1, $2, $3, $4, 'cancelled',
                jsonb_build_object('source', 'RopAbortSubmit')
            )
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(&tenant_id)
        .bind(queue_id)
        .bind(trace_id)
        .execute(&mut *tx)
        .await?;

        let modseq = self
            .allocate_mail_modseq_in_tx(&mut tx, &tenant_id, account_id)
            .await?;
        let principals =
            Self::affected_mail_principals_in_tx(&mut tx, &tenant_id, account_id).await?;
        Self::insert_mail_change_log_in_tx(
            &mut tx,
            &tenant_id,
            Some(account_id),
            None,
            "submission",
            queue_id,
            "updated",
            modseq,
            &principals,
            serde_json::json!({
                "messageId": message_id,
                "status": "cancelled"
            }),
        )
        .await?;
        self.insert_audit(&mut tx, &tenant_id, audit).await?;
        Self::emit_mail_change(&mut tx, &tenant_id, account_id).await?;
        tx.commit().await?;

        Ok(CancelSubmissionResult::Cancelled)
    }

    pub async fn delete_draft_message(
        &self,
        account_id: Uuid,
        message_id: Uuid,
        audit: AuditEntryInput,
    ) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        self.delete_draft_message_in_tx(&mut tx, &tenant_id, account_id, message_id)
            .await?;
        self.insert_audit(&mut tx, &tenant_id, audit).await?;
        Self::emit_mail_change(&mut tx, &tenant_id, account_id).await?;
        tx.commit().await?;
        Ok(())
    }

    pub(crate) async fn account_identity_for_id(
        &self,
        account_id: Uuid,
    ) -> Result<AccountIdentity> {
        let row = sqlx::query(
            r#"
            SELECT id, primary_email, display_name
            FROM accounts
            WHERE id = $1
            LIMIT 1
            "#,
        )
        .bind(account_id)
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| anyhow!("account not found"))?;

        Ok(AccountIdentity {
            id: row.try_get("id")?,
            email: row.try_get("primary_email")?,
            display_name: row.try_get("display_name")?,
        })
    }

    pub(crate) async fn load_account_identity_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, Postgres>,
        tenant_id: &Uuid,
        account_id: Uuid,
    ) -> Result<AccountIdentity> {
        let row = sqlx::query(
            r#"
            SELECT id, primary_email, display_name
            FROM accounts
            WHERE tenant_id = $1 AND id = $2
            LIMIT 1
            "#,
        )
        .bind(tenant_id)
        .bind(account_id)
        .fetch_optional(&mut **tx)
        .await?
        .ok_or_else(|| anyhow!("account not found"))?;

        Ok(AccountIdentity {
            id: row.try_get("id")?,
            email: row.try_get("primary_email")?,
            display_name: row.try_get("display_name")?,
        })
    }

    pub(crate) async fn load_account_identity_by_email_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, Postgres>,
        tenant_id: &Uuid,
        email: &str,
    ) -> Result<AccountIdentity> {
        let row = sqlx::query(
            r#"
            SELECT id, primary_email, display_name
            FROM accounts
            WHERE tenant_id = $1 AND normalized_primary_email = $2
            LIMIT 1
            "#,
        )
        .bind(tenant_id)
        .bind(email)
        .fetch_optional(&mut **tx)
        .await?
        .ok_or_else(|| anyhow!("grantee account not found in the same tenant"))?;

        Ok(AccountIdentity {
            id: row.try_get("id")?,
            email: row.try_get("primary_email")?,
            display_name: row.try_get("display_name")?,
        })
    }

    async fn ensure_same_tenant_account_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, Postgres>,
        tenant_id: &Uuid,
        account_id: Uuid,
    ) -> Result<()> {
        self.load_account_identity_in_tx(tx, tenant_id, account_id)
            .await
            .map(|_| ())
    }

    async fn has_sender_right_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, Postgres>,
        tenant_id: &Uuid,
        owner_account_id: Uuid,
        grantee_account_id: Uuid,
        sender_right: SenderDelegationRight,
    ) -> Result<bool> {
        sqlx::query_scalar::<_, bool>(
            r#"
            SELECT EXISTS(
                SELECT 1
                FROM sender_rights
                WHERE tenant_id = $1
                  AND owner_account_id = $2
                  AND grantee_account_id = $3
                  AND sender_right = $4
                  AND identity_id IS NULL
            )
            "#,
        )
        .bind(tenant_id)
        .bind(owner_account_id)
        .bind(grantee_account_id)
        .bind(sender_right.as_str())
        .fetch_one(&mut **tx)
        .await
        .map_err(Into::into)
    }

    async fn resolve_submission_authorization_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, Postgres>,
        tenant_id: &Uuid,
        input: &SubmitMessageInput,
    ) -> Result<ResolvedSubmissionAuthorization> {
        let owner = self
            .load_account_identity_in_tx(tx, tenant_id, input.account_id)
            .await?;
        let submitted_by = self
            .load_account_identity_in_tx(tx, tenant_id, input.submitted_by_account_id)
            .await?;
        let requested_from = normalize_email(&input.from_address);
        let requested_sender = input
            .sender_address
            .as_deref()
            .map(normalize_email)
            .filter(|value| !value.is_empty());
        let owner_display_name = owner.display_name.clone();
        let submitted_by_display_name = submitted_by.display_name.clone();

        if requested_from.is_empty() {
            bail!("from_address is required");
        }

        if owner.id == submitted_by.id {
            if requested_from != owner.email {
                bail!("from email must match authenticated account");
            }
            if let Some(sender_address) = requested_sender {
                if sender_address != submitted_by.email {
                    bail!("sender email must match authenticated account");
                }
            }
            return Ok(ResolvedSubmissionAuthorization {
                submitted_by,
                from_address: requested_from,
                from_display: trim_optional_text(input.from_display.as_deref())
                    .or_else(|| Some(owner_display_name.clone())),
                sender_address: None,
                sender_display: None,
                authorization_kind: SenderAuthorizationKind::SelfSend,
            });
        }

        if requested_from != owner.email {
            bail!("from email must match delegated mailbox");
        }

        if let Some(sender_address) = requested_sender {
            if sender_address != submitted_by.email {
                bail!("sender email must match authenticated account");
            }
            if !self
                .has_sender_right_in_tx(
                    tx,
                    tenant_id,
                    owner.id,
                    submitted_by.id,
                    SenderDelegationRight::SendOnBehalf,
                )
                .await?
            {
                bail!("send on behalf is not granted for this mailbox");
            }
            return Ok(ResolvedSubmissionAuthorization {
                submitted_by,
                from_address: requested_from,
                from_display: trim_optional_text(input.from_display.as_deref())
                    .or_else(|| Some(owner_display_name.clone())),
                sender_address: Some(sender_address),
                sender_display: trim_optional_text(input.sender_display.as_deref())
                    .or_else(|| Some(submitted_by_display_name)),
                authorization_kind: SenderAuthorizationKind::SendOnBehalf,
            });
        }

        if !self
            .has_sender_right_in_tx(
                tx,
                tenant_id,
                owner.id,
                submitted_by.id,
                SenderDelegationRight::SendAs,
            )
            .await?
        {
            bail!("send as is not granted for this mailbox");
        }

        Ok(ResolvedSubmissionAuthorization {
            submitted_by,
            from_address: requested_from,
            from_display: trim_optional_text(input.from_display.as_deref())
                .or_else(|| Some(owner_display_name)),
            sender_address: None,
            sender_display: None,
            authorization_kind: SenderAuthorizationKind::SendAs,
        })
    }

    pub async fn find_submission_account_by_email_in_same_tenant(
        &self,
        reference_account_id: Uuid,
        email: &str,
    ) -> Result<Option<SubmissionAccountIdentity>> {
        let tenant_id = self.tenant_id_for_account_id(reference_account_id).await?;
        let normalized_email = normalize_email(email);
        if normalized_email.is_empty() {
            return Ok(None);
        }

        let row = sqlx::query(
            r#"
            SELECT id, primary_email, display_name
            FROM accounts
            WHERE tenant_id = $1 AND normalized_primary_email = $2
            LIMIT 1
            "#,
        )
        .bind(&tenant_id)
        .bind(&normalized_email)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(|row| SubmissionAccountIdentity {
            account_id: row.get("id"),
            email: row.get("primary_email"),
            display_name: row.get("display_name"),
        }))
    }

    async fn delete_draft_message_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, Postgres>,
        tenant_id: &Uuid,
        account_id: Uuid,
        message_id: Uuid,
    ) -> Result<()> {
        let modseq = self
            .allocate_mail_modseq_in_tx(tx, tenant_id, account_id)
            .await?;
        let row = sqlx::query(
            r#"
            SELECT mm.id, mm.mailbox_id, mm.thread_id, mm.imap_uid, mm.is_seen
            FROM mailbox_messages mm
            JOIN mailboxes mb
              ON mb.tenant_id = mm.tenant_id
             AND mb.account_id = mm.account_id
             AND mb.id = mm.mailbox_id
            WHERE mm.tenant_id = $1
              AND mm.account_id = $2
              AND mm.message_id = $3
              AND mm.visibility = 'visible'
              AND mb.role = 'drafts'
            LIMIT 1
            "#,
        )
        .bind(tenant_id)
        .bind(account_id)
        .bind(message_id)
        .fetch_optional(&mut **tx)
        .await?
        .ok_or_else(|| anyhow!("draft not found"))?;
        let mailbox_message_id: Uuid = row.try_get("id")?;
        let mailbox_id: Uuid = row.try_get("mailbox_id")?;
        let imap_uid: i64 = row.try_get("imap_uid")?;
        let is_seen: bool = row.try_get("is_seen")?;
        let principals = Self::affected_mail_principals_in_tx(tx, tenant_id, account_id).await?;
        let cursor = Self::insert_mail_change_log_in_tx(
            tx,
            tenant_id,
            Some(account_id),
            Some(mailbox_id),
            "mailbox_message",
            mailbox_message_id,
            "destroyed",
            modseq,
            &principals,
            serde_json::json!({
                "messageId": message_id,
                "threadId": row.try_get::<Uuid, _>("thread_id")?,
                "imapUid": imap_uid
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
        .bind(mailbox_id)
        .bind(mailbox_message_id)
        .bind(message_id)
        .bind(imap_uid)
        .bind(modseq)
        .bind(cursor)
        .execute(&mut **tx)
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
        .bind(tenant_id)
        .bind(account_id)
        .bind(mailbox_message_id)
        .bind(modseq)
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
        .bind(mailbox_id)
        .bind(is_seen)
        .bind(modseq)
        .execute(&mut **tx)
        .await?;
        Self::recalculate_mailbox_counts_in_tx(tx, tenant_id, account_id, mailbox_id, modseq)
            .await?;

        Ok(())
    }
}
