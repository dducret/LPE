use anyhow::{anyhow, bail, Result};
use chrono::Utc;
use sqlx::{Postgres, Row};
use uuid::Uuid;

use crate::{
    mail::{parse_calendar_meeting_request, parse_calendar_meeting_response_with_content_sha256},
    mapi_message_identity::rotate_active_mapi_message_identity_in_tx,
    normalize_email, normalize_subject, sha256_hex, trim_optional_text, AuditEntryInput, Storage,
};

mod delegate_preferences;
mod delegation;
mod helpers;
mod meeting_request;
mod mime;
mod source;
mod source_mutation;
mod source_patch;
mod types;

use helpers::{exact_editor_submission_input, insert_visible_recipient, SubmissionSourceBehavior};
use types::{
    canonical_submission_phases, source_protocol_sql, submission_authorization_kind_sql,
    CanonicalSubmissionPhase, ResolvedSubmissionAuthorization,
};
pub(crate) use types::{
    normalize_bcc_recipients, normalize_visible_recipients, participants_normalized,
    push_recipients, sender_authorization_kind_from_str, sender_identity_id, AccountIdentity,
};
pub use types::{
    AttachmentUploadInput, CancelSubmissionResult, CanonicalEwsDelegateInput, DelegatePreferences,
    DelegatePreferencesPatch, MailboxAccountAccess, MailboxDelegationGrant,
    MailboxDelegationGrantInput, MailboxDelegationOverview, MailboxFolderDelegationGrantInput,
    SavedDraftMessage, SenderAuthorizationKind, SenderDelegationGrant, SenderDelegationGrantInput,
    SenderDelegationRight, SenderIdentity, SubmissionAccountIdentity,
    SubmissionMessageCustomPropertyInput, SubmissionSourcePatch, SubmitMessageInput,
    SubmittedMessage, SubmittedRecipientInput,
};

impl Storage {
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
        sqlx::query("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
            .execute(&mut *tx)
            .await?;
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
        let draft_claim = if input.draft_message_id.is_some() {
            Some(
                self.claim_submission_source_in_tx(
                    &mut tx,
                    &tenant_id,
                    input.account_id,
                    message_id,
                    false,
                )
                .await?,
            )
        } else {
            None
        };
        if draft_claim.is_some() {
            self.ensure_message_shared_state_is_private_to_account_in_tx(
                &mut tx,
                &tenant_id,
                input.account_id,
                message_id,
            )
            .await?;
        }
        let mut effective_attachments = if let Some(claim) = draft_claim.as_ref() {
            if input.replace_attachments {
                Vec::new()
            } else {
                self.fetch_claimed_submission_source_attachment_inputs_in_tx(
                    &mut tx,
                    &tenant_id,
                    input.account_id,
                    claim,
                )
                .await?
            }
        } else {
            Vec::new()
        };
        effective_attachments.extend(input.attachments.iter().cloned());
        let calendar_request = parse_calendar_meeting_request(&effective_attachments);
        let (calendar_response, authorized_calendar_response_content_sha256) =
            match parse_calendar_meeting_response_with_content_sha256(&effective_attachments) {
                Some((response, content_sha256))
                    if meeting_request::validate_outbound_response_envelope(
                        &response,
                        &authorization.from_address,
                        &visible_recipients,
                        &bcc_recipients,
                    )
                    .is_ok() =>
                {
                    (Some(response), Some(content_sha256))
                }
                _ => (None, None),
            };
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
                    authorized_calendar_response_content_sha256 = $11,
                    calendar_response_processed = FALSE,
                    has_attachments = CASE WHEN $10 THEN FALSE ELSE has_attachments END
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
            .bind(input.replace_attachments)
            .bind(authorized_calendar_response_content_sha256.as_deref())
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
            if input.replace_attachments {
                self.delete_claimed_submission_source_attachments_in_tx(
                    &mut tx,
                    &tenant_id,
                    draft_claim
                        .as_ref()
                        .ok_or_else(|| anyhow!("draft source claim is missing"))?,
                    None,
                )
                .await?;
            }
            let membership = sqlx::query(
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
            if membership.rows_affected() != 1 {
                bail!("draft membership changed while saving");
            }
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
                    authorized_calendar_response_content_sha256, normalized_subject,
                    sent_at, received_at, size_octets, has_attachments
                )
                VALUES (
                    $1, $2, $3, $4, $5, $6,
                    $7, $8, NULL, NOW(), $9, FALSE
                )
                "#,
            )
            .bind(message_id)
            .bind(&tenant_id)
            .bind(domain_id)
            .bind(blob_id)
            .bind(input.internet_message_id)
            .bind(sha256_hex(raw_message.as_bytes()))
            .bind(authorized_calendar_response_content_sha256.as_deref())
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
                    id, tenant_id, message_id, owner_account_id, address, display_name, ordinal, metadata_scope
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, 'audit-compliance')
                "#,
            )
            .bind(Uuid::new_v4())
            .bind(&tenant_id)
            .bind(message_id)
            .bind(input.account_id)
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
        if draft_claim.is_some() {
            sqlx::query(
                r#"
                UPDATE calendar_mail_classifications
                SET needs_reclassification = TRUE,
                    scheduling_mime_part_id = NULL,
                    updated_at = NOW()
                WHERE tenant_id = $1 AND message_id = $2
                "#,
            )
            .bind(&tenant_id)
            .bind(message_id)
            .execute(&mut *tx)
            .await?;
        }
        if draft_claim.is_none() {
            self.persist_new_calendar_mail_classification_in_tx(
                &mut tx,
                &tenant_id,
                input.account_id,
                message_id,
                calendar_request.as_ref(),
                calendar_response.as_ref(),
            )
            .await?;
        }
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
        if !existing_draft_update {
            self.mark_calendar_mail_classification_applied_in_tx(
                &mut tx,
                &tenant_id,
                input.account_id,
                message_id,
            )
            .await?;
        }
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
        if existing_draft_update {
            rotate_active_mapi_message_identity_in_tx(
                &mut tx,
                &tenant_id,
                input.account_id,
                message_id,
            )
            .await?;
        }

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

    /// Submits a new message, or atomically replaces and submits the visible
    /// Drafts/Outbox source named by `draft_message_id`.
    pub async fn submit_message(
        &self,
        input: SubmitMessageInput,
        audit: AuditEntryInput,
    ) -> Result<SubmittedMessage> {
        self.submit_message_with_source_behavior(
            input,
            audit,
            SubmissionSourceBehavior::ReplaceWithInput,
            None,
        )
        .await
    }

    /// Atomically applies a complete editor overlay plus selective protocol
    /// metadata changes to one claimed Drafts/Outbox source and submits it.
    pub async fn submit_message_with_source_patch(
        &self,
        input: SubmitMessageInput,
        patch: SubmissionSourcePatch,
        audit: AuditEntryInput,
    ) -> Result<SubmittedMessage> {
        if input.draft_message_id.is_none() {
            bail!("submission source patch requires a Drafts/Outbox source");
        }
        self.submit_message_with_source_behavior(
            input,
            audit,
            SubmissionSourceBehavior::ReplaceWithInput,
            Some(patch),
        )
        .await
    }

    async fn submit_message_with_source_behavior(
        &self,
        mut input: SubmitMessageInput,
        audit: AuditEntryInput,
        source_behavior: SubmissionSourceBehavior,
        source_patch: Option<SubmissionSourcePatch>,
    ) -> Result<SubmittedMessage> {
        let tenant_id = self.tenant_id_for_account_id(input.account_id).await?;
        let mut tx = self.pool.begin().await?;
        sqlx::query("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
            .execute(&mut *tx)
            .await?;
        let source_context = input.draft_message_id.map(|message_id| {
            (
                message_id,
                input.account_id,
                input.submitted_by_account_id,
                input.source.clone(),
            )
        });
        let source_claim = if let Some((message_id, account_id, _, _)) = source_context.as_ref() {
            // Keep PostgreSQL READ COMMITTED here. The shared claim helper first
            // locks the parent message row, which serializes cross-account copy;
            // its following membership statement must see a copier's commit if
            // that parent lock had to wait.
            Some(
                self.claim_submission_source_in_tx(
                    &mut tx,
                    &tenant_id,
                    *account_id,
                    *message_id,
                    true,
                )
                .await?,
            )
        } else {
            None
        };

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

        if let (Some(claim), Some((_, account_id, submitted_by_account_id, source))) =
            (source_claim.as_ref(), source_context.as_ref())
        {
            if source_behavior == SubmissionSourceBehavior::ReplaceWithInput {
                self.ensure_message_shared_state_is_private_to_account_in_tx(
                    &mut tx,
                    &tenant_id,
                    *account_id,
                    claim.message_id,
                )
                .await?;
            }
            if let Some(patch) = source_patch.as_ref() {
                self.apply_claimed_submission_source_patch_in_tx(&mut tx, &tenant_id, claim, patch)
                    .await?;
            }
            let persisted = self
                .load_claimed_submission_source_input_in_tx(
                    &mut tx,
                    &tenant_id,
                    *account_id,
                    *submitted_by_account_id,
                    source,
                    claim,
                )
                .await?;
            if source_behavior == SubmissionSourceBehavior::ReplaceWithInput {
                let update_authorization = self
                    .resolve_submission_authorization_in_tx(&mut tx, &tenant_id, &input)
                    .await?;
                self.replace_claimed_submission_source_in_tx(
                    &mut tx,
                    &tenant_id,
                    claim,
                    &input,
                    &update_authorization,
                )
                .await?;
                input = exact_editor_submission_input(input, persisted, &update_authorization);
            } else {
                input = persisted;
            }
        }

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
        let authorization = self
            .resolve_submission_authorization_in_tx(&mut tx, &tenant_id, &input)
            .await?;
        meeting_request::validate_outbound_scheduling_body_in_tx(
            self,
            &mut tx,
            &tenant_id,
            input.account_id,
            &authorization.from_address,
            input.source.eq_ignore_ascii_case("mapi-submit-message")
                && input.draft_message_id.is_none(),
            &subject,
            &body_text,
            input.body_html_sanitized.as_deref(),
            &input.attachments,
            &visible_recipients,
            &bcc_recipients,
        )
        .await?;
        let submission_attachments = input.attachments.clone();
        let calendar_request = parse_calendar_meeting_request(&submission_attachments);
        let (calendar_response, authorized_calendar_response_content_sha256) =
            match parse_calendar_meeting_response_with_content_sha256(&submission_attachments) {
                Some((response, content_sha256)) => (Some(response), Some(content_sha256)),
                None => (None, None),
            };

        let message_id = Uuid::new_v4();
        let thread_id = Uuid::new_v4();
        let outbound_queue_id = Uuid::new_v4();
        let submitted_at = Utc::now();
        let mime_identity = mime::resolve_submission_mime_identity(
            input.internet_message_id.as_deref(),
            source_claim
                .as_ref()
                .and_then(|claim| claim.internet_message_id.as_deref()),
            source_claim
                .as_ref()
                .and_then(|claim| claim.date_header.as_deref()),
            message_id,
            &authorization.from_address,
            submitted_at,
        );
        let participants_normalized =
            participants_normalized(&authorization.from_address, &visible_recipients);
        let domain_id = self
            .load_account_domain_id_in_tx(&mut tx, &tenant_id, input.account_id)
            .await?;
        let raw_message = mime::render_submission_raw_message(
            &authorization.from_address,
            &input,
            &body_text,
            &submission_attachments,
            &mime_identity,
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
                            authorized_calendar_response_content_sha256, normalized_subject,
                            sent_at, received_at, size_octets, has_attachments
                        )
                        VALUES (
                            $1, $2, $3, $4, $5, $6,
                            $7, $8, $10, $10, $9, FALSE
                        )
                        "#,
                    )
                    .bind(message_id)
                    .bind(&tenant_id)
                    .bind(domain_id)
                    .bind(blob_id)
                    .bind(&mime_identity.internet_message_id)
                    .bind(sha256_hex(raw_message.as_bytes()))
                    .bind(authorized_calendar_response_content_sha256.as_deref())
                    .bind(&subject)
                    .bind(input.size_octets.max(0))
                    .bind(submitted_at)
                    .execute(&mut *tx)
                    .await?;

                    if let Some(claim) = source_claim.as_ref() {
                        self.copy_claimed_submission_source_custom_properties_to_sent_in_tx(
                            &mut tx,
                            &tenant_id,
                            input.account_id,
                            claim,
                            message_id,
                        )
                        .await?;
                    }

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
                                id, tenant_id, message_id, owner_account_id, address, display_name, ordinal, metadata_scope
                            )
                            VALUES ($1, $2, $3, $4, $5, $6, $7, 'audit-compliance')
                            "#,
                        )
                        .bind(Uuid::new_v4())
                        .bind(&tenant_id)
                        .bind(message_id)
                        .bind(input.account_id)
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
                        &submission_attachments,
                    )
                    .await?;
                    self.persist_new_calendar_mail_classification_in_tx(
                        &mut tx,
                        &tenant_id,
                        input.account_id,
                        message_id,
                        calendar_request.as_ref(),
                        calendar_response.as_ref(),
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
                    self.mark_calendar_mail_classification_applied_in_tx(
                        &mut tx,
                        &tenant_id,
                        input.account_id,
                        message_id,
                    )
                    .await?;
                    if let Some(claim) = source_claim.as_ref() {
                        self.copy_claimed_submission_source_followup_to_sent_in_tx(
                            &mut tx,
                            &tenant_id,
                            input.account_id,
                            claim,
                            mailbox_message_id,
                        )
                        .await?;
                    }
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
                CanonicalSubmissionPhase::DeleteSubmissionSource => {
                    if let Some(claim) = source_claim.as_ref() {
                        self.delete_submission_source_message_in_tx(
                            &mut tx,
                            &tenant_id,
                            input.account_id,
                            claim,
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

    /// Submits the exact persisted version of one visible Drafts/Outbox source.
    /// Caller-provided message content is intentionally not part of this API.
    pub async fn submit_draft_message(
        &self,
        account_id: Uuid,
        draft_message_id: Uuid,
        submitted_by_account_id: Uuid,
        source: &str,
        audit: AuditEntryInput,
    ) -> Result<SubmittedMessage> {
        self.submit_message_with_source_behavior(
            SubmitMessageInput {
                draft_message_id: Some(draft_message_id),
                account_id,
                submitted_by_account_id,
                source: source.trim().to_lowercase(),
                from_display: None,
                from_address: String::new(),
                sender_display: None,
                sender_address: None,
                to: Vec::new(),
                cc: Vec::new(),
                bcc: Vec::new(),
                subject: String::new(),
                body_text: String::new(),
                body_html_sanitized: None,
                internet_message_id: None,
                mime_blob_ref: None,
                size_octets: 0,
                unread: None,
                flagged: None,
                replace_attachments: false,
                attachments: Vec::new(),
            },
            audit,
            SubmissionSourceBehavior::UsePersisted,
            None,
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
        sqlx::query("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
            .execute(&mut *tx)
            .await?;
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
}
