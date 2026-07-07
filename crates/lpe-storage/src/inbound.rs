use anyhow::{bail, Result};
use lpe_core::sieve::{
    evaluate_script, ExecutionOutcome as SieveExecutionOutcome,
    MessageContext as SieveMessageContext, VacationAction,
};
use lpe_domain::{
    InboundDeliveryRequest, InboundDeliveryResponse, MailboxDisplayName, MailboxNamePolicy,
};
use sha2::{Digest, Sha256};
use sqlx::{Postgres, Row};
use std::collections::{BTreeMap, BTreeSet};
use uuid::Uuid;

use crate::mail::{parse_header_recipients, parse_headers_map, parse_message_attachments};
use crate::shared::allocate_uid_validity;
use crate::{submission, AttachmentUploadInput, AuditEntryInput, Storage, SubmittedRecipientInput};

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
    pub async fn verify_local_recipient(&self, recipient: &str) -> Result<bool> {
        let recipient = crate::normalize_email(recipient);
        if recipient.is_empty() {
            bail!("recipient is required");
        }

        sqlx::query_scalar::<_, bool>(
            r#"
            SELECT EXISTS (
                SELECT 1
                FROM accounts
                WHERE normalized_primary_email = $1
            )
            "#,
        )
        .bind(&recipient)
        .fetch_one(&self.pool)
        .await
        .map_err(Into::into)
    }

    pub async fn deliver_inbound_message(
        &self,
        mut request: InboundDeliveryRequest,
    ) -> Result<InboundDeliveryResponse> {
        request.trace_id = request.trace_id.trim().to_string();
        if request.trace_id.is_empty() {
            bail!("trace_id is required");
        }

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

        let mut tx = self.pool.begin().await?;
        lock_inbound_trace_delivery(&mut tx, &request.trace_id).await?;
        if let Some(existing) =
            existing_inbound_delivery_response_in_tx(&mut tx, &request.trace_id).await?
        {
            tx.commit().await?;
            return Ok(existing);
        }

        let account_rows = sqlx::query(
            r#"
            SELECT id, tenant_id, primary_email, display_name
            FROM accounts
            WHERE normalized_primary_email = ANY($1)
            ORDER BY primary_email ASC
        "#,
        )
        .bind(&rcpt_to)
        .fetch_all(&mut *tx)
        .await?;

        let mut accepted = Vec::new();
        let mut rejected = Vec::new();
        let mut stored_messages = Vec::new();
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
            let tenant_id: Uuid = row.try_get("tenant_id")?;
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
                        "INBOX",
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
                stored_messages.push((account_id, message_id));
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
            &crate::PLATFORM_TENANT_ID,
            AuditEntryInput {
                actor: "lpe-ct".to_string(),
                action: audit_action.to_string(),
                subject: request.trace_id.clone(),
            },
        )
        .await?;
        tx.commit().await?;
        let mut followup_errors = Vec::new();

        for (account_id, message_id) in &stored_messages {
            if let Err(error) = self
                .apply_conversation_actions_to_jmap_email(*account_id, *message_id, "lpe-ct")
                .await
            {
                followup_errors.push(error.to_string());
            }
        }

        for followup in followups {
            if let Err(error) = self.dispatch_sieve_followups(&followup).await {
                followup_errors.push(error.to_string());
            }
        }

        Ok(inbound_delivery_response(
            accepted,
            rejected,
            stored_messages,
            followup_errors,
        ))
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
            if !followup.sender_address.is_empty()
                && !followup
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
        tenant_id: &Uuid,
        account_id: Uuid,
        mailbox_id: Uuid,
        thread_id: Uuid,
        message_id: Uuid,
        request: &InboundDeliveryRequest,
        mail_from: &str,
        subject: &str,
        _preview: &str,
        size_octets: i64,
        body_text: &str,
        participants: &str,
        visible_recipients: &[(&'static str, SubmittedRecipientInput)],
        attachments: &[AttachmentUploadInput],
    ) -> Result<()> {
        let domain_id = self
            .load_account_domain_id_in_tx(tx, tenant_id, account_id)
            .await?;
        let blob_id = self
            .store_message_blob_in_tx(
                tx,
                tenant_id,
                domain_id,
                "raw_message",
                "message/rfc822",
                &request.raw_message,
            )
            .await?;
        let sent_at = crate::mail::parse_message_date_header(&request.raw_message);

        sqlx::query(
            r#"
            INSERT INTO messages (
                id, tenant_id, domain_id, blob_id, internet_message_id, message_hash,
                normalized_subject, sent_at, received_at, size_octets, has_attachments
            )
            VALUES (
                $1, $2, $3, $4, $5, $6,
                $7, COALESCE($8::timestamptz, NOW()), NOW(), $9, FALSE
            )
            "#,
        )
        .bind(message_id)
        .bind(tenant_id)
        .bind(domain_id)
        .bind(blob_id)
        .bind(request.internet_message_id.as_deref())
        .bind(crate::sha256_hex(&request.raw_message))
        .bind(subject)
        .bind(sent_at.as_deref())
        .bind(size_octets.max(0))
        .execute(&mut **tx)
        .await?;

        let header_count = self
            .replace_message_headers_in_tx(tx, tenant_id, message_id, &request.raw_message)
            .await?;

        self.upsert_message_body_in_tx(tx, tenant_id, domain_id, message_id, body_text, None)
            .await?;

        sqlx::query(
            r#"
            INSERT INTO message_headers (id, tenant_id, message_id, header_name, header_value, ordinal)
            VALUES ($1, $2, $3, 'x-lpe-ct-trace-id', $4, $5)
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(tenant_id)
        .bind(message_id)
        .bind(&request.trace_id)
        .bind(header_count as i32)
        .execute(&mut **tx)
        .await?;
        sqlx::query(
            r#"
            INSERT INTO message_recipients (
                id, tenant_id, message_id, role, address, display_name, ordinal
            )
            VALUES ($1, $2, $3, 'from', $4, NULL, 0)
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(tenant_id)
        .bind(message_id)
        .bind(mail_from)
        .execute(&mut **tx)
        .await?;

        for (ordinal, (kind, recipient_value)) in visible_recipients.iter().enumerate() {
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
            .bind(kind)
            .bind(&recipient_value.address)
            .bind(recipient_value.display_name.as_deref())
            .bind(ordinal as i32)
            .execute(&mut **tx)
            .await?;
        }

        self.ingest_message_attachments_in_tx(tx, tenant_id, account_id, message_id, attachments)
            .await?;
        let membership_id = self
            .allocate_mailbox_membership_in_tx(
                tx, tenant_id, account_id, mailbox_id, message_id, thread_id, "", false, false,
                false, "created",
            )
            .await?;
        self.assign_message_attachments_membership_in_tx(
            tx,
            tenant_id,
            account_id,
            message_id,
            membership_id,
        )
        .await?;
        Self::upsert_mail_search_document_in_tx(
            tx,
            tenant_id,
            account_id,
            membership_id,
            message_id,
            subject,
            participants,
            body_text,
            "",
        )
        .await?;
        Ok(())
    }

    async fn ensure_named_mailbox(
        &self,
        tx: &mut sqlx::Transaction<'_, Postgres>,
        tenant_id: &Uuid,
        account_id: Uuid,
        display_name: &str,
        _retention_days: i32,
    ) -> Result<Uuid> {
        let display_name = MailboxDisplayName::new(display_name)
            .map_err(|_| anyhow::anyhow!("fileinto target mailbox is invalid"))?
            .into_string();
        let requested_key = MailboxNamePolicy::canonical_key(&display_name);
        let rows = sqlx::query(
            r#"
            SELECT id, display_name
            FROM mailboxes
            WHERE tenant_id = $1 AND account_id = $2
            "#,
        )
        .bind(tenant_id)
        .bind(account_id)
        .fetch_all(&mut **tx)
        .await?;
        for row in rows {
            let existing_name = row.try_get::<String, _>("display_name")?;
            if requested_key.collides_with(&MailboxNamePolicy::canonical_key(&existing_name)) {
                return row.try_get("id").map_err(Into::into);
            }
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
                id, tenant_id, account_id, role, display_name, sort_order, uid_validity
            )
            VALUES ($1, $2, $3, 'custom', $4, $5, $6)
            "#,
        )
        .bind(mailbox_id)
        .bind(tenant_id)
        .bind(account_id)
        .bind(&display_name)
        .bind(sort_order)
        .bind(allocate_uid_validity())
        .execute(&mut **tx)
        .await?;
        Ok(mailbox_id)
    }
}

async fn lock_inbound_trace_delivery(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    trace_id: &str,
) -> Result<()> {
    let (class_id, object_id) = inbound_trace_advisory_lock_keys(trace_id);
    sqlx::query("SELECT pg_advisory_xact_lock($1, $2)")
        .bind(class_id)
        .bind(object_id)
        .execute(&mut **tx)
        .await?;
    Ok(())
}

async fn existing_inbound_delivery_response_in_tx(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    trace_id: &str,
) -> Result<Option<InboundDeliveryResponse>> {
    let rows = sqlx::query(
        r#"
        SELECT a.primary_email, m.id
        FROM messages m
        JOIN mailbox_messages mm
          ON mm.tenant_id = m.tenant_id
         AND mm.message_id = m.id
         AND mm.visibility = 'visible'
        JOIN accounts a
          ON a.tenant_id = mm.tenant_id
         AND a.id = mm.account_id
        JOIN message_headers h
          ON h.tenant_id = m.tenant_id
         AND h.message_id = m.id
         AND lower(h.header_name) = 'x-lpe-ct-trace-id'
        WHERE h.header_value = $1
        ORDER BY a.primary_email ASC, m.id ASC
        "#,
    )
    .bind(trace_id.trim())
    .fetch_all(&mut **tx)
    .await?;

    if !rows.is_empty() {
        let mut mailbox_addresses = BTreeSet::new();
        let mut message_ids = Vec::new();
        for row in rows {
            mailbox_addresses.insert(row.try_get::<String, _>("primary_email")?);
            let message_id: Uuid = row.try_get("id")?;
            message_ids.push(format!("message:{message_id}"));
        }
        let mut delivered_mailboxes = mailbox_addresses.into_iter().collect::<Vec<_>>();
        delivered_mailboxes.extend(message_ids);
        return Ok(Some(duplicate_inbound_delivery_response(
            trace_id,
            delivered_mailboxes,
        )));
    }

    let committed = sqlx::query_scalar::<_, bool>(
        r#"
        SELECT EXISTS (
            SELECT 1
            FROM audit_events
            WHERE tenant_id = $1
              AND actor = 'lpe-ct'
              AND action IN ('mail.inbound.delivered', 'mail.inbound.delivery-rejected')
              AND subject = $2
        )
        "#,
    )
    .bind(crate::PLATFORM_TENANT_ID)
    .bind(trace_id)
    .fetch_one(&mut **tx)
    .await?;

    Ok(committed
        .then(|| duplicate_inbound_delivery_response(trace_id, vec![format!("trace:{trace_id}")])))
}

fn duplicate_inbound_delivery_response(
    trace_id: &str,
    delivered_mailboxes: Vec<String>,
) -> InboundDeliveryResponse {
    InboundDeliveryResponse {
        accepted: true,
        delivered_mailboxes,
        detail: Some(format!(
            "duplicate inbound delivery trace replay suppressed for {trace_id}"
        )),
    }
}

fn inbound_delivery_response(
    accepted: Vec<String>,
    rejected: Vec<String>,
    stored_messages: Vec<(Uuid, Uuid)>,
    followup_errors: Vec<String>,
) -> InboundDeliveryResponse {
    let accepted_by_core = !accepted.is_empty();
    let mut delivered_mailboxes = accepted;
    delivered_mailboxes.extend(
        rejected
            .into_iter()
            .map(|recipient| format!("rejected:{recipient}")),
    );
    delivered_mailboxes.extend(
        stored_messages
            .into_iter()
            .map(|(_, id)| format!("message:{id}")),
    );

    InboundDeliveryResponse {
        accepted: accepted_by_core,
        delivered_mailboxes,
        detail: if followup_errors.is_empty() {
            None
        } else {
            Some(format!(
                "post-delivery errors: {}",
                followup_errors.join(" | ")
            ))
        },
    }
}

fn inbound_trace_advisory_lock_keys(trace_id: &str) -> (i32, i32) {
    let mut hasher = Sha256::new();
    hasher.update(b"lpe-inbound-trace-v1\0");
    hasher.update(trace_id.trim().as_bytes());
    let digest = hasher.finalize();
    (
        i32::from_be_bytes([digest[0], digest[1], digest[2], digest[3]]),
        i32::from_be_bytes([digest[4], digest[5], digest[6], digest[7]]),
    )
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inbound_trace_id_helpers_normalize_whitespace() {
        assert_eq!(
            inbound_trace_advisory_lock_keys("trace-1"),
            inbound_trace_advisory_lock_keys(" trace-1 ")
        );
        assert_ne!(
            inbound_trace_advisory_lock_keys("trace-1"),
            inbound_trace_advisory_lock_keys("trace-2")
        );
    }

    #[test]
    fn duplicate_inbound_response_returns_committed_receipt() {
        let response = duplicate_inbound_delivery_response(
            "trace-1",
            vec![
                "user@example.test".to_string(),
                "message:00000000-0000-0000-0000-000000000001".to_string(),
            ],
        );

        assert!(response.accepted);
        assert_eq!(response.delivered_mailboxes.len(), 2);
        assert!(response
            .detail
            .as_deref()
            .unwrap_or_default()
            .contains("duplicate inbound delivery trace replay suppressed"));
    }

    #[test]
    fn inbound_response_rejects_when_no_recipient_was_accepted() {
        let response = inbound_delivery_response(
            Vec::new(),
            vec!["missing@example.test".to_string()],
            Vec::new(),
            Vec::new(),
        );

        assert!(!response.accepted);
        assert_eq!(
            response.delivered_mailboxes,
            vec!["rejected:missing@example.test".to_string()]
        );
    }

    #[test]
    fn inbound_response_accepts_when_at_least_one_recipient_was_accepted() {
        let response = inbound_delivery_response(
            vec!["user@example.test".to_string()],
            vec!["missing@example.test".to_string()],
            vec![(Uuid::nil(), Uuid::nil())],
            Vec::new(),
        );

        assert!(response.accepted);
        assert!(response
            .delivered_mailboxes
            .contains(&"user@example.test".to_string()));
        assert!(response
            .delivered_mailboxes
            .contains(&"rejected:missing@example.test".to_string()));
        assert!(response
            .delivered_mailboxes
            .contains(&format!("message:{}", Uuid::nil())));
    }
}
