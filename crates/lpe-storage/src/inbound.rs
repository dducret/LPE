use anyhow::{bail, Result};
use lpe_core::sieve::{
    evaluate_script, ExecutionOutcome as SieveExecutionOutcome,
    MessageContext as SieveMessageContext, VacationAction,
};
use lpe_domain::{InboundDeliveryRequest, InboundDeliveryResponse, TransportDeliveryStatus};
use sha2::{Digest, Sha256};
use sqlx::{Postgres, Row};
use std::collections::BTreeMap;
use uuid::Uuid;

use crate::{
    submission, AttachmentUploadInput, AuditEntryInput, Storage, SubmittedRecipientInput,
};
use crate::mail::{parse_header_recipients, parse_headers_map, parse_message_attachments};

const MAX_SIEVE_REDIRECTS_PER_MESSAGE: usize = 4;
const DEFAULT_SIEVE_MAILBOX_RETENTION_DAYS: i32 = 365;

#[derive(Debug, Clone)]
struct SieveFollowUp {
    account_id: Uuid,
    account_email: String,
    account_display_name: String,
    redirects: Vec<String>,
    vacation: Option<VacationAction>,
    subject: String,
    body_text: String,
    attachments: Vec<AttachmentUploadInput>,
    sender_address: String,
}

impl Storage {
    pub async fn deliver_inbound_message(
        &self,
        request: InboundDeliveryRequest,
    ) -> Result<InboundDeliveryResponse> {
        let mail_from = crate::normalize_email(&request.mail_from);
        let subject = crate::normalize_subject(&request.subject);
        let body_text = request.body_text.trim().to_string();
        let headers = parse_headers_map(&request.raw_message);
        let rcpt_to = request
            .rcpt_to
            .iter()
            .map(|recipient| crate::normalize_email(recipient))
            .filter(|recipient| !recipient.is_empty())
            .collect::<Vec<_>>();

        if mail_from.is_empty() {
            bail!("mail_from is required");
        }
        if rcpt_to.is_empty() {
            bail!("at least one recipient is required");
        }

        let visible_to = parse_header_recipients(&request.raw_message, "to");
        let visible_cc = parse_header_recipients(&request.raw_message, "cc");
        let mut visible_recipients = Vec::with_capacity(visible_to.len() + visible_cc.len());
        submission::push_recipients(&mut visible_recipients, "to", &visible_to);
        submission::push_recipients(&mut visible_recipients, "cc", &visible_cc);
        let participants = submission::participants_normalized(&mail_from, &visible_recipients);
        let preview = crate::preview_text(&body_text);
        let size_octets = request.raw_message.len() as i64;

        let account_rows = sqlx::query(
            r#"
            SELECT id, tenant_id, primary_email, display_name
            FROM accounts
            WHERE lower(primary_email) = ANY($1)
            ORDER BY primary_email ASC
            "#,
        )
        .bind(&rcpt_to)
        .fetch_all(&self.pool)
        .await?;

        let mut accepted = Vec::new();
        let mut rejected = Vec::new();
        let mut stored_message_ids = Vec::new();
        let mut tx = self.pool.begin().await?;
        let thread_id = Uuid::new_v4();
        let attachments = parse_message_attachments(&request.raw_message)?;
        let mut followups = Vec::new();

        for recipient in &rcpt_to {
            let Some(row) = account_rows.iter().find(|row| {
                row.try_get::<String, _>("primary_email")
                    .map(|value| crate::normalize_email(&value) == *recipient)
                    .unwrap_or(false)
            }) else {
                rejected.push(recipient.clone());
                continue;
            };

            let account_id: Uuid = row.try_get("id")?;
            let tenant_id: String = row.try_get("tenant_id")?;
            let account_email: String = row.try_get("primary_email")?;
            let account_display_name: String = row.try_get("display_name")?;
            let sieve_outcome = self
                .evaluate_inbound_sieve(account_id, &mail_from, recipient, &headers, &account_email)
                .await?;
            let had_sieve_actions = sieve_outcome.file_into.is_some()
                || sieve_outcome.discard
                || !sieve_outcome.redirects.is_empty()
                || sieve_outcome.vacation.is_some();

            if !sieve_outcome.discard {
                let mailbox_id = if let Some(folder_name) = sieve_outcome.file_into.as_deref() {
                    self.ensure_named_mailbox(
                        &mut tx,
                        &tenant_id,
                        account_id,
                        folder_name,
                        DEFAULT_SIEVE_MAILBOX_RETENTION_DAYS,
                    )
                    .await?
                } else {
                    self.ensure_mailbox(
                        &mut tx,
                        &tenant_id,
                        account_id,
                        "inbox",
                        "Inbox",
                        0,
                        DEFAULT_SIEVE_MAILBOX_RETENTION_DAYS,
                    )
                    .await?
                };
                let message_id = Uuid::new_v4();
                self.store_inbound_message_in_tx(
                    &mut tx,
                    &tenant_id,
                    account_id,
                    mailbox_id,
                    thread_id,
                    message_id,
                    &request,
                    &mail_from,
                    &subject,
                    &preview,
                    size_octets,
                    &body_text,
                    &participants,
                    &visible_recipients,
                    &attachments,
                )
                .await?;
                stored_message_ids.push(message_id);
            }

            if !sieve_outcome.redirects.is_empty() || sieve_outcome.vacation.is_some() {
                followups.push(SieveFollowUp {
                    account_id,
                    account_email: crate::normalize_email(&account_email),
                    account_display_name,
                    redirects: sieve_outcome.redirects,
                    vacation: sieve_outcome.vacation,
                    subject: subject.clone(),
                    body_text: body_text.clone(),
                    attachments: attachments.clone(),
                    sender_address: mail_from.clone(),
                });
            }

            if had_sieve_actions {
                self.insert_audit(
                    &mut tx,
                    &tenant_id,
                    AuditEntryInput {
                        actor: account_email.clone(),
                        action: "mail.sieve.applied".to_string(),
                        subject: format!("{}:{}", request.trace_id, recipient),
                    },
                )
                .await?;
            }

            accepted.push(recipient.clone());
        }

        let audit_action = if accepted.is_empty() {
            "mail.inbound.delivery-rejected"
        } else {
            "mail.inbound.delivered"
        };
        self.insert_audit(
            &mut tx,
            crate::PLATFORM_TENANT_ID,
            AuditEntryInput {
                actor: "lpe-ct".to_string(),
                action: audit_action.to_string(),
                subject: request.trace_id.clone(),
            },
        )
        .await?;
        tx.commit().await?;
        let mut followup_errors = Vec::new();

        for followup in followups {
            if let Err(error) = self.dispatch_sieve_followups(&followup).await {
                followup_errors.push(error.to_string());
            }
        }

        Ok(InboundDeliveryResponse {
            trace_id: request.trace_id,
            status: if accepted.is_empty() {
                TransportDeliveryStatus::Failed
            } else {
                TransportDeliveryStatus::Relayed
            },
            accepted_recipients: accepted,
            rejected_recipients: rejected,
            stored_message_ids,
            detail: if followup_errors.is_empty() {
                None
            } else {
                Some(format!(
                    "sieve follow-up errors: {}",
                    followup_errors.join(" | ")
                ))
            },
        })
    }

    async fn evaluate_inbound_sieve(
        &self,
        account_id: Uuid,
        envelope_from: &str,
        envelope_to: &str,
        headers: &std::collections::HashMap<String, String>,
        account_email: &str,
    ) -> Result<SieveExecutionOutcome> {
        let Some(script) = self.fetch_active_sieve_script(account_id).await? else {
            return Ok(SieveExecutionOutcome::default());
        };
        let Ok(script) = lpe_core::sieve::parse_script(&script.content) else {
            return Ok(SieveExecutionOutcome::default());
        };

        let mut normalized_headers = BTreeMap::new();
        for (name, value) in headers {
            normalized_headers.insert(name.to_lowercase(), vec![value.clone()]);
        }
        if !normalized_headers.contains_key("to") {
            normalized_headers.insert("to".to_string(), vec![account_email.to_string()]);
        }

        evaluate_script(
            &script,
            &SieveMessageContext {
                envelope_from: envelope_from.to_string(),
                envelope_to: envelope_to.to_string(),
                headers: normalized_headers,
            },
        )
    }

    async fn dispatch_sieve_followups(&self, followup: &SieveFollowUp) -> Result<()> {
        for redirect in followup
            .redirects
            .iter()
            .take(MAX_SIEVE_REDIRECTS_PER_MESSAGE)
        {
            if redirect.eq_ignore_ascii_case(&followup.account_email) {
                continue;
            }
            self.submit_message(
                crate::SubmitMessageInput {
                    draft_message_id: None,
                    account_id: followup.account_id,
                    submitted_by_account_id: followup.account_id,
                    source: "sieve-redirect".to_string(),
                    from_display: Some(followup.account_display_name.clone()),
                    from_address: followup.account_email.clone(),
                    sender_display: None,
                    sender_address: None,
                    to: vec![SubmittedRecipientInput {
                        address: redirect.clone(),
                        display_name: None,
                    }],
                    cc: Vec::new(),
                    bcc: Vec::new(),
                    subject: followup.subject.clone(),
                    body_text: followup.body_text.clone(),
                    body_html_sanitized: None,
                    internet_message_id: None,
                    mime_blob_ref: None,
                    size_octets: estimate_generated_message_size(
                        &followup.subject,
                        &followup.body_text,
                        &followup.attachments,
                    ),
                    unread: Some(false),
                    flagged: Some(false),
                    attachments: followup.attachments.clone(),
                },
                AuditEntryInput {
                    actor: followup.account_email.clone(),
                    action: "mail.sieve.redirect".to_string(),
                    subject: redirect.clone(),
                },
            )
            .await?;
        }

        if let Some(vacation) = &followup.vacation {
            if !followup
                .sender_address
                .eq_ignore_ascii_case(&followup.account_email)
                && self
                    .should_send_sieve_vacation(
                        followup.account_id,
                        &followup.sender_address,
                        vacation,
                    )
                    .await?
            {
                self.submit_message(
                    crate::SubmitMessageInput {
                        draft_message_id: None,
                        account_id: followup.account_id,
                        submitted_by_account_id: followup.account_id,
                        source: "sieve-vacation".to_string(),
                        from_display: Some(followup.account_display_name.clone()),
                        from_address: followup.account_email.clone(),
                        sender_display: None,
                        sender_address: None,
                        to: vec![SubmittedRecipientInput {
                            address: followup.sender_address.clone(),
                            display_name: None,
                        }],
                        cc: Vec::new(),
                        bcc: Vec::new(),
                        subject: vacation
                            .subject
                            .clone()
                            .unwrap_or_else(|| format!("Re: {}", followup.subject)),
                        body_text: vacation.reason.clone(),
                        body_html_sanitized: None,
                        internet_message_id: None,
                        mime_blob_ref: None,
                        size_octets: vacation.reason.len() as i64,
                        unread: Some(false),
                        flagged: Some(false),
                        attachments: Vec::new(),
                    },
                    AuditEntryInput {
                        actor: followup.account_email.clone(),
                        action: "mail.sieve.vacation".to_string(),
                        subject: followup.sender_address.clone(),
                    },
                )
                .await?;
            }
        }

        Ok(())
    }

    async fn should_send_sieve_vacation(
        &self,
        account_id: Uuid,
        sender_address: &str,
        vacation: &VacationAction,
    ) -> Result<bool> {
        let tenant_id = self.tenant_id_for_account_id(account_id).await?;
        let sender_address = crate::normalize_email(sender_address);
        let response_key = hash_sieve_vacation_key(vacation);
        let updated = sqlx::query(
            r#"
            INSERT INTO sieve_vacation_responses (
                tenant_id, account_id, sender_address, response_key, last_sent_at
            )
            VALUES ($1, $2, $3, $4, NOW())
            ON CONFLICT (tenant_id, account_id, sender_address, response_key) DO UPDATE SET
                last_sent_at = EXCLUDED.last_sent_at
            WHERE sieve_vacation_responses.last_sent_at
                <= NOW() - make_interval(days => GREATEST(1, $5))
            RETURNING last_sent_at
            "#,
        )
        .bind(&tenant_id)
        .bind(account_id)
        .bind(&sender_address)
        .bind(&response_key)
        .bind(vacation.days as i32)
        .fetch_optional(&self.pool)
        .await?;

        Ok(updated.is_some())
    }

    async fn store_inbound_message_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, Postgres>,
        tenant_id: &str,
        account_id: Uuid,
        mailbox_id: Uuid,
        thread_id: Uuid,
        message_id: Uuid,
        request: &InboundDeliveryRequest,
        mail_from: &str,
        subject: &str,
        preview: &str,
        size_octets: i64,
        body_text: &str,
        participants: &str,
        visible_recipients: &[(&'static str, SubmittedRecipientInput)],
        attachments: &[AttachmentUploadInput],
    ) -> Result<()> {
        let mime_blob_ref = format!("lpe-ct-inbound:{}:{message_id}", request.trace_id);
        let modseq = self
            .allocate_mail_modseq_in_tx(tx, tenant_id, account_id)
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
                $7, NOW(), NULL, NULL, $8, NULL,
                NULL, 'self', $3, $9, $10, TRUE, FALSE, FALSE, $11, $12,
                'lpe-ct', 'stored'
            )
            "#,
        )
        .bind(message_id)
        .bind(tenant_id)
        .bind(account_id)
        .bind(mailbox_id)
        .bind(thread_id)
        .bind(request.internet_message_id.as_deref())
        .bind(modseq)
        .bind(mail_from)
        .bind(subject)
        .bind(preview)
        .bind(size_octets.max(0))
        .bind(&mime_blob_ref)
        .execute(&mut **tx)
        .await?;

        sqlx::query(
            r#"
            INSERT INTO message_bodies (
                message_id, body_text, body_html_sanitized, participants_normalized,
                language_code, content_hash, search_vector
            )
            VALUES ($1, $2, NULL, $3, NULL, $4, to_tsvector('simple', $5))
            "#,
        )
        .bind(message_id)
        .bind(body_text)
        .bind(participants)
        .bind(format!("inbound:{}:{message_id}", request.trace_id))
        .bind(format!("{subject} {body_text} {participants}"))
        .execute(&mut **tx)
        .await?;

        for (ordinal, (kind, recipient_value)) in visible_recipients.iter().enumerate() {
            sqlx::query(
                r#"
                INSERT INTO message_recipients (
                    id, tenant_id, message_id, kind, address, display_name, ordinal
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                "#,
            )
            .bind(Uuid::new_v4())
            .bind(tenant_id)
            .bind(message_id)
            .bind(kind)
            .bind(&recipient_value.address)
            .bind(recipient_value.display_name.as_deref())
            .bind(ordinal as i32)
            .execute(&mut **tx)
            .await?;
        }

        self.ingest_message_attachments_in_tx(tx, tenant_id, account_id, message_id, attachments)
            .await
    }

    async fn ensure_named_mailbox(
        &self,
        tx: &mut sqlx::Transaction<'_, Postgres>,
        tenant_id: &str,
        account_id: Uuid,
        display_name: &str,
        retention_days: i32,
    ) -> Result<Uuid> {
        let display_name = display_name.trim();
        if display_name.is_empty() {
            bail!("fileinto target mailbox is required");
        }
        if let Some(mailbox_id) = sqlx::query_scalar::<_, Uuid>(
            r#"
            SELECT id
            FROM mailboxes
            WHERE tenant_id = $1 AND account_id = $2 AND lower(display_name) = lower($3)
            LIMIT 1
            "#,
        )
        .bind(tenant_id)
        .bind(account_id)
        .bind(display_name)
        .fetch_optional(&mut **tx)
        .await?
        {
            return Ok(mailbox_id);
        }

        let sort_order = sqlx::query_scalar::<_, i32>(
            r#"
            SELECT COALESCE(MAX(sort_order), 0) + 1
            FROM mailboxes
            WHERE tenant_id = $1 AND account_id = $2
            "#,
        )
        .bind(tenant_id)
        .bind(account_id)
        .fetch_one(&mut **tx)
        .await?;
        let mailbox_id = Uuid::new_v4();
        sqlx::query(
            r#"
            INSERT INTO mailboxes (
                id, tenant_id, account_id, role, display_name, sort_order, retention_days
            )
            VALUES ($1, $2, $3, '', $4, $5, $6)
            "#,
        )
        .bind(mailbox_id)
        .bind(tenant_id)
        .bind(account_id)
        .bind(display_name)
        .bind(sort_order)
        .bind(retention_days)
        .execute(&mut **tx)
        .await?;
        Ok(mailbox_id)
    }
}

fn estimate_generated_message_size(
    subject: &str,
    body_text: &str,
    attachments: &[AttachmentUploadInput],
) -> i64 {
    let attachments_size = attachments
        .iter()
        .map(|attachment| attachment.blob_bytes.len() as i64)
        .sum::<i64>();
    (subject.len() as i64) + (body_text.len() as i64) + attachments_size
}

fn hash_sieve_vacation_key(vacation: &VacationAction) -> String {
    let mut hasher = Sha256::new();
    hasher.update(vacation.subject.as_deref().unwrap_or_default().as_bytes());
    hasher.update(b"\n");
    hasher.update(vacation.reason.as_bytes());
    hasher.update(b"\n");
    hasher.update(vacation.days.to_string().as_bytes());
    format!("{:x}", hasher.finalize())
}
