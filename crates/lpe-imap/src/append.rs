use anyhow::{anyhow, bail, Result};
use lpe_magika::{
    collect_mime_attachment_parts, Detector, ExpectedKind, IngressContext, PolicyDecision,
    ValidationRequest, Validator,
};
use lpe_storage::{
    mail::parse_rfc822_message, AuditEntryInput, JmapImportedEmailInput, JmapMailbox,
    SubmitMessageInput,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use uuid::Uuid;

use crate::{
    parse::{parse_literal_size, tokenize},
    render::mailbox_name_matches,
    Session, UID_VALIDITY,
};

impl<S: crate::store::ImapStore, D: Detector> Session<S, D> {
    pub(crate) async fn handle_append<R, W>(
        &mut self,
        reader: &mut BufReader<R>,
        writer: &mut W,
        tag: &str,
        arguments: &str,
    ) -> Result<bool>
    where
        R: AsyncReadExt + Unpin,
        W: AsyncWriteExt + Unpin,
    {
        let tokens = tokenize(arguments)?;
        if tokens.len() < 2 {
            bail!("APPEND expects mailbox and literal size");
        }
        let principal = self.require_auth()?.clone();
        let mailbox_name = &tokens[0];
        let mailbox = self.resolve_append_mailbox(mailbox_name).await?;
        let append_to_drafts = mailbox.role == "drafts";
        let append_to_sent = mailbox.role == "sent";
        let literal_size = parse_literal_size(tokens.last().unwrap())?;

        writer.write_all(b"+ Ready for literal data\r\n").await?;
        writer.flush().await?;

        let mut literal = vec![0u8; literal_size];
        reader.read_exact(&mut literal).await?;
        let mut line_end = [0u8; 2];
        reader.read_exact(&mut line_end).await?;

        validate_append_attachments(&self.validator, &literal)?;
        if append_to_sent {
            let parsed = parse_rfc822_message(&literal).ok();
            let sent_messages = self
                .store
                .fetch_imap_emails(principal.account_id, mailbox.id)
                .await?;
            let append_uid = sent_append_ack_uid(
                &sent_messages,
                parsed
                    .as_ref()
                    .and_then(|message| message.message_id.as_deref()),
            );
            if let Some(uid) = append_uid {
                writer
                    .write_all(
                        format!(
                            "{tag} OK [APPENDUID {} {}] APPEND completed\r\n",
                            UID_VALIDITY, uid
                        )
                        .as_bytes(),
                    )
                    .await?;
                writer.flush().await?;
                return Ok(true);
            }

            writer
                .write_all(format!("{tag} OK APPEND completed\r\n").as_bytes())
                .await?;
            writer.flush().await?;
            return Ok(true);
        }

        let parsed = parse_rfc822_message(&literal)?;
        let from_display = parsed
            .from
            .as_ref()
            .and_then(|address| address.display_name.clone())
            .or_else(|| Some(principal.display_name.clone()));
        let from_address = parsed
            .from
            .map(|address| address.email)
            .unwrap_or_else(|| principal.email.clone());

        if append_to_drafts {
            let saved = self
                .store
                .save_draft_message(
                    SubmitMessageInput {
                        draft_message_id: None,
                        account_id: principal.account_id,
                        submitted_by_account_id: principal.account_id,
                        source: "imap-append".to_string(),
                        from_display,
                        from_address,
                        sender_display: None,
                        sender_address: None,
                        to: parsed
                            .to
                            .into_iter()
                            .map(|recipient| lpe_storage::SubmittedRecipientInput {
                                address: recipient.email,
                                display_name: recipient.display_name,
                            })
                            .collect(),
                        cc: parsed
                            .cc
                            .into_iter()
                            .map(|recipient| lpe_storage::SubmittedRecipientInput {
                                address: recipient.email,
                                display_name: recipient.display_name,
                            })
                            .collect(),
                        bcc: Vec::new(),
                        subject: parsed.subject,
                        body_text: parsed.body_text,
                        body_html_sanitized: None,
                        internet_message_id: parsed.message_id,
                        mime_blob_ref: Some(format!("imap-append:{}", Uuid::new_v4())),
                        size_octets: literal.len() as i64,
                        unread: Some(false),
                        flagged: Some(false),
                        attachments: parsed.attachments,
                    },
                    AuditEntryInput {
                        actor: principal.email.clone(),
                        action: "imap-append".to_string(),
                        subject: "draft message append".to_string(),
                    },
                )
                .await?;

            let appended = self
                .store
                .fetch_imap_emails(principal.account_id, saved.draft_mailbox_id)
                .await?
                .into_iter()
                .find(|email| email.id == saved.message_id)
                .ok_or_else(|| anyhow!("saved draft message not found"))?;

            if matches!(self.selected.as_ref(), Some(selected) if selected.mailbox_id == saved.draft_mailbox_id)
            {
                self.refresh_selected().await?;
            }

            writer
                .write_all(
                    format!(
                        "{tag} OK [APPENDUID {} {}] APPEND completed\r\n",
                        UID_VALIDITY, appended.uid
                    )
                    .as_bytes(),
                )
                .await?;
            writer.flush().await?;
            return Ok(true);
        }

        let appended = self
            .store
            .import_imap_email(
                JmapImportedEmailInput {
                    account_id: principal.account_id,
                    submitted_by_account_id: principal.account_id,
                    mailbox_id: mailbox.id,
                    source: "imap-append".to_string(),
                    from_display,
                    from_address,
                    sender_display: None,
                    sender_address: None,
                    to: parsed
                        .to
                        .into_iter()
                        .map(|recipient| lpe_storage::SubmittedRecipientInput {
                            address: recipient.email,
                            display_name: recipient.display_name,
                        })
                        .collect(),
                    cc: parsed
                        .cc
                        .into_iter()
                        .map(|recipient| lpe_storage::SubmittedRecipientInput {
                            address: recipient.email,
                            display_name: recipient.display_name,
                        })
                        .collect(),
                    bcc: Vec::new(),
                    subject: parsed.subject,
                    body_text: parsed.body_text,
                    body_html_sanitized: None,
                    internet_message_id: parsed.message_id,
                    mime_blob_ref: format!("imap-append:{}", Uuid::new_v4()),
                    size_octets: literal.len() as i64,
                    received_at: None,
                    attachments: parsed.attachments,
                },
                AuditEntryInput {
                    actor: principal.email.clone(),
                    action: "imap-append".to_string(),
                    subject: format!("append message to mailbox {}", mailbox.name),
                },
            )
            .await?;

        if matches!(self.selected.as_ref(), Some(selected) if selected.mailbox_id == mailbox.id) {
            self.refresh_selected().await?;
        }

        writer
            .write_all(
                format!(
                    "{tag} OK [APPENDUID {} {}] APPEND completed\r\n",
                    UID_VALIDITY, appended.uid
                )
                .as_bytes(),
            )
            .await?;
        writer.flush().await?;
        Ok(true)
    }

    async fn resolve_append_mailbox(&self, mailbox_name: &str) -> Result<JmapMailbox> {
        let principal = self.require_auth()?;
        self.store
            .ensure_imap_mailboxes(principal.account_id)
            .await?
            .into_iter()
            .find(|candidate| mailbox_name_matches(&candidate.name, &candidate.role, mailbox_name))
            .ok_or_else(|| anyhow!("mailbox not found"))
    }
}

fn validate_append_attachments<D: Detector>(validator: &Validator<D>, bytes: &[u8]) -> Result<()> {
    for attachment in collect_mime_attachment_parts(bytes)? {
        let outcome = validator.validate_bytes(
            ValidationRequest {
                ingress_context: IngressContext::ImapAppend,
                declared_mime: attachment.declared_mime.clone(),
                filename: attachment.filename.clone(),
                expected_kind: ExpectedKind::Any,
            },
            &attachment.bytes,
        )?;
        if outcome.policy_decision != PolicyDecision::Accept {
            bail!(
                "IMAP APPEND blocked by Magika validation for {:?}: {}",
                attachment.filename,
                outcome.reason
            );
        }
    }
    Ok(())
}

fn sent_append_ack_uid(
    sent_messages: &[lpe_storage::ImapEmail],
    appended_message_id: Option<&str>,
) -> Option<u32> {
    appended_message_id
        .and_then(|message_id| {
            sent_messages
                .iter()
                .find(|email| {
                    email
                        .internet_message_id
                        .as_deref()
                        .is_some_and(|stored_message_id| {
                            message_ids_match(stored_message_id, message_id)
                        })
                })
                .map(|email| email.uid)
        })
        .or_else(|| sent_messages.iter().map(|email| email.uid).max())
}

fn message_ids_match(stored: &str, appended: &str) -> bool {
    normalize_message_id(stored).eq_ignore_ascii_case(&normalize_message_id(appended))
}

fn normalize_message_id(value: &str) -> &str {
    value.trim().trim_start_matches('<').trim_end_matches('>')
}
