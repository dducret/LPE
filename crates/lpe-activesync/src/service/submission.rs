use anyhow::{anyhow, bail, Result};
use axum::{http::HeaderMap, response::Response};
use lpe_storage::{AuditEntryInput, SubmitMessageInput, SubmittedRecipientInput};
use uuid::Uuid;

use crate::{
    message::{default_sender, parse_mime_message},
    protocol::{ActiveSyncCommand, ActiveSyncStatus},
    response::{empty_response, is_message_rfc822, wbxml_response},
    store::ActiveSyncStore,
    types::AuthenticatedPrincipal,
    wbxml::{decode_wbxml, encode_wbxml, WbxmlNode},
};

use super::{command_status_response, validate_mime_attachments, ActiveSyncService};
impl<S: ActiveSyncStore> ActiveSyncService<S> {
    pub(super) async fn handle_send_mail(
        &self,
        principal: &AuthenticatedPrincipal,
        protocol_version: &str,
        headers: &HeaderMap,
        body: &[u8],
    ) -> Result<Response> {
        let mime_payload = if is_message_rfc822(headers) {
            body.to_vec()
        } else {
            let request = decode_wbxml(body)?;
            if request.name != "SendMail" {
                return command_status_response(protocol_version, 21, "SendMail", "103");
            }
            match request.child("Mime") {
                Some(node) => node.text_value().as_bytes().to_vec(),
                None => return command_status_response(protocol_version, 21, "SendMail", "103"),
            }
        };

        let parsed = match parse_mime_message(&mime_payload) {
            Ok(parsed) => parsed,
            Err(_) => return command_status_response(protocol_version, 21, "SendMail", "107"),
        };
        if validate_mime_attachments(&mime_payload).is_err() {
            return command_status_response(protocol_version, 21, "SendMail", "107");
        }
        let mailbox_access = self
            .mailbox_access_for_from_address(
                principal,
                parsed.from.as_ref().map(|mailbox| mailbox.address.as_str()),
            )
            .await;
        let Ok(mailbox_access) = mailbox_access else {
            return command_status_response(protocol_version, 21, "SendMail", "166");
        };
        let from_display = parsed
            .from
            .as_ref()
            .and_then(|mailbox| mailbox.display_name.clone())
            .or_else(|| Some(mailbox_access.display_name.clone()));
        let from_address = parsed
            .from
            .map(|mailbox| mailbox.address)
            .unwrap_or_else(|| mailbox_access.email.clone());
        let (sender_display, sender_address) = match parsed.sender {
            Some(sender) => (sender.display_name, Some(sender.address)),
            None => default_sender(&mailbox_access, principal, None, None),
        };
        let submitted = self
            .store
            .submit_message(
                SubmitMessageInput {
                    draft_message_id: None,
                    account_id: mailbox_access.account_id,
                    submitted_by_account_id: principal.account_id,
                    source: "activesync-sendmail".to_string(),
                    from_display,
                    from_address,
                    sender_display,
                    sender_address,
                    to: parsed.to,
                    cc: parsed.cc,
                    bcc: parsed.bcc,
                    subject: parsed.subject,
                    body_text: parsed.body_text,
                    body_html_sanitized: None,
                    internet_message_id: parsed.internet_message_id,
                    mime_blob_ref: Some(format!("activesync-mime:{}", Uuid::new_v4())),
                    size_octets: mime_payload.len() as i64,
                    unread: Some(false),
                    flagged: Some(false),
                    attachments: parsed.attachments,
                },
                AuditEntryInput {
                    actor: principal.email.clone(),
                    action: "activesync-sendmail".to_string(),
                    subject: "native client message submission".to_string(),
                },
            )
            .await;
        if submitted.is_err() {
            return command_status_response(protocol_version, 21, "SendMail", "120");
        }

        if is_message_rfc822(headers) {
            Ok(empty_response())
        } else {
            wbxml_response(protocol_version, Vec::new())
        }
    }

    pub(super) async fn handle_smart_compose(
        &self,
        principal: &AuthenticatedPrincipal,
        protocol_version: &str,
        request: &WbxmlNode,
        command: ActiveSyncCommand,
    ) -> Result<Response> {
        let command_name = command.as_str();
        if request.name != command_name {
            return command_status_response(protocol_version, 21, command_name, "103");
        }

        let source = self.resolve_source_message(principal, request).await;
        let Ok((source_mailbox_access, source_message)) = source else {
            return command_status_response(protocol_version, 21, command_name, "150");
        };
        let mime_payload = request
            .child("Mime")
            .map(|node| node.text_value().as_bytes().to_vec())
            .ok_or_else(|| anyhow!("{command_name} is missing MIME content"));
        let Ok(mime_payload) = mime_payload else {
            return command_status_response(protocol_version, 21, command_name, "103");
        };
        if validate_mime_attachments(&mime_payload).is_err() {
            return command_status_response(protocol_version, 21, command_name, "107");
        }
        let parsed = match parse_mime_message(&mime_payload) {
            Ok(parsed) => parsed,
            Err(_) => return command_status_response(protocol_version, 21, command_name, "107"),
        };
        let mailbox_access = match self
            .mailbox_access_for_from_address(
                principal,
                parsed.from.as_ref().map(|mailbox| mailbox.address.as_str()),
            )
            .await
        {
            Ok(access) => access,
            Err(_) => source_mailbox_access.clone(),
        };

        let (to, cc) =
            if parsed.to.is_empty() && parsed.cc.is_empty() && command_name == "SmartReply" {
                (
                    reply_recipients(principal.email.as_str(), &source_message),
                    Vec::new(),
                )
            } else {
                (parsed.to, parsed.cc)
            };
        let mut attachments = parsed.attachments;
        if command_name == "SmartForward" {
            attachments.extend(
                self.load_message_attachment_uploads(
                    source_mailbox_access.account_id,
                    source_message.id,
                )
                .await?,
            );
        }

        let subject = if parsed.subject.trim().is_empty() {
            default_reply_subject(command_name, &source_message.subject)
        } else {
            parsed.subject
        };
        let body_text =
            merge_smart_body(command_name, &parsed.body_text, &source_message.body_text);
        let (sender_display, sender_address) = match parsed.sender {
            Some(sender) => (sender.display_name, Some(sender.address)),
            None => default_sender(&mailbox_access, principal, None, None),
        };
        let submitted = self
            .store
            .submit_message(
                SubmitMessageInput {
                    draft_message_id: None,
                    account_id: mailbox_access.account_id,
                    submitted_by_account_id: principal.account_id,
                    source: format!("activesync-{}", command_name.to_ascii_lowercase()),
                    from_display: parsed
                        .from
                        .as_ref()
                        .and_then(|mailbox| mailbox.display_name.clone())
                        .or_else(|| Some(mailbox_access.display_name.clone())),
                    from_address: parsed
                        .from
                        .map(|mailbox| mailbox.address)
                        .unwrap_or_else(|| mailbox_access.email.clone()),
                    sender_display,
                    sender_address,
                    to,
                    cc,
                    bcc: parsed.bcc,
                    subject,
                    body_text,
                    body_html_sanitized: None,
                    internet_message_id: parsed.internet_message_id,
                    mime_blob_ref: Some(format!("activesync-mime:{}", Uuid::new_v4())),
                    size_octets: mime_payload.len() as i64,
                    unread: Some(false),
                    flagged: Some(false),
                    attachments,
                },
                AuditEntryInput {
                    actor: principal.email.clone(),
                    action: format!("activesync-{}", command_name.to_ascii_lowercase()),
                    subject: source_message.id.to_string(),
                },
            )
            .await;
        if submitted.is_err() {
            return command_status_response(protocol_version, 21, command_name, "120");
        }

        let mut response = WbxmlNode::new(21, command_name);
        response.push(WbxmlNode::with_text(
            21,
            "Status",
            ActiveSyncStatus::Success.as_str(),
        ));
        wbxml_response(protocol_version, encode_wbxml(&response))
    }

    async fn resolve_source_message(
        &self,
        principal: &AuthenticatedPrincipal,
        request: &WbxmlNode,
    ) -> Result<(lpe_storage::MailboxAccountAccess, lpe_storage::JmapEmail)> {
        let source = request
            .child("Source")
            .ok_or_else(|| anyhow!("compose command is missing Source"))?;
        let message_id = source
            .child("ItemId")
            .or_else(|| source.child("LongId"))
            .map(|node| node.text_value().trim().to_string())
            .ok_or_else(|| anyhow!("compose command is missing ItemId"))?;
        let message_id = Uuid::parse_str(&message_id)?;
        for access in self.mailbox_accesses(principal).await? {
            if let Some(email) = self
                .store
                .fetch_jmap_emails(access.account_id, &[message_id])
                .await?
                .into_iter()
                .next()
            {
                return Ok((access, email));
            }
        }
        bail!("source message not found")
    }

    async fn load_message_attachment_uploads(
        &self,
        account_id: Uuid,
        message_id: Uuid,
    ) -> Result<Vec<lpe_storage::AttachmentUploadInput>> {
        let attachments = self
            .store
            .fetch_activesync_message_attachments(account_id, message_id)
            .await?;
        let mut uploads = Vec::with_capacity(attachments.len());
        for attachment in attachments {
            let Some(content) = self
                .store
                .fetch_activesync_attachment_content(account_id, &attachment.file_reference)
                .await?
            else {
                continue;
            };
            uploads.push(lpe_storage::AttachmentUploadInput {
                file_name: content.file_name,
                media_type: content.media_type,
                disposition: Some("attachment".to_string()),
                content_id: None,
                blob_bytes: content.blob_bytes,
            });
        }
        Ok(uploads)
    }
}

fn reply_recipients(
    principal_email: &str,
    source_message: &lpe_storage::JmapEmail,
) -> Vec<SubmittedRecipientInput> {
    if source_message
        .from_address
        .eq_ignore_ascii_case(principal_email)
    {
        return source_message
            .to
            .iter()
            .filter(|recipient| !recipient.address.eq_ignore_ascii_case(principal_email))
            .map(|recipient| SubmittedRecipientInput {
                address: recipient.address.clone(),
                display_name: recipient.display_name.clone(),
            })
            .collect();
    }

    vec![SubmittedRecipientInput {
        address: source_message.from_address.clone(),
        display_name: source_message.from_display.clone(),
    }]
}

fn default_reply_subject(command_name: &str, original_subject: &str) -> String {
    let normalized = original_subject.trim();
    let prefix = if command_name == "SmartForward" {
        "Fwd:"
    } else {
        "Re:"
    };
    if normalized
        .to_ascii_lowercase()
        .starts_with(&prefix[..2].to_ascii_lowercase())
    {
        normalized.to_string()
    } else {
        format!("{prefix} {normalized}").trim().to_string()
    }
}

fn merge_smart_body(command_name: &str, composed: &str, original: &str) -> String {
    let label = if command_name == "SmartForward" {
        "Forwarded message"
    } else {
        "Original message"
    };
    let composed = composed.trim();
    let original = original.trim();
    if composed.is_empty() {
        original.to_string()
    } else if original.is_empty() {
        composed.to_string()
    } else {
        format!("{composed}\n\n----- {label} -----\n{original}")
    }
}
