use anyhow::{anyhow, bail, Result};
use lpe_magika::{
    collect_mime_attachment_parts, Detector, ExpectedKind, IngressContext, PolicyDecision,
    ValidationRequest, Validator,
};
use lpe_storage::{mail::parse_rfc822_message, AuditEntryInput, SubmitMessageInput};
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
        let mailbox_name = &tokens[0];
        if !mailbox_name_matches("Drafts", "drafts", mailbox_name) {
            bail!("APPEND is only allowed for Drafts");
        }
        let literal_size = parse_literal_size(tokens.last().unwrap())?;

        writer.write_all(b"+ Ready for literal data\r\n").await?;
        writer.flush().await?;

        let mut literal = vec![0u8; literal_size];
        reader.read_exact(&mut literal).await?;
        let mut line_end = [0u8; 2];
        reader.read_exact(&mut line_end).await?;

        validate_append_attachments(&self.validator, &literal)?;
        let parsed = parse_rfc822_message(&literal)?;
        let principal = self.require_auth()?;
        let from_display = parsed
            .from
            .as_ref()
            .and_then(|address| address.display_name.clone())
            .or_else(|| Some(principal.display_name.clone()));
        let from_address = parsed
            .from
            .map(|address| address.email)
            .unwrap_or_else(|| principal.email.clone());

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

        if matches!(self.selected.as_ref(), Some(selected) if selected.mailbox_name.eq_ignore_ascii_case("Drafts"))
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
        Ok(true)
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
