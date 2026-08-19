use super::super::*;
use anyhow::Context;
use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use lpe_magika::{ExpectedKind, IngressContext, PolicyDecision, ValidationRequest};
use sha2::{Digest, Sha256};

const MAX_TRANSFER_ITEMS: usize = 100;
const MAX_UPLOAD_BASE64_BYTES: usize = 25 * 1024 * 1024;

impl<S, V> ExchangeService<S, V>
where
    S: ExchangeStore + Clone + Send + Sync + 'static,
    V: Detector + Clone + Send + Sync + 'static,
{
    pub(in crate::service) async fn upload_items(
        &self,
        principal: &AccountPrincipal,
        request: &str,
    ) -> Result<String> {
        let uploads = requested_transfer_uploads(request)?;
        if uploads.len() > MAX_TRANSFER_ITEMS {
            bail!("UploadItems supports at most {MAX_TRANSFER_ITEMS} items");
        }
        if uploads.iter().map(|upload| upload.data.len()).sum::<usize>() > MAX_UPLOAD_BASE64_BYTES {
            bail!("UploadItems Data payload exceeds the 25 MiB bounded transfer limit");
        }
        let mailboxes = self.store.fetch_jmap_mailboxes(principal.account_id).await?;
        let mut inputs = Vec::with_capacity(uploads.len());
        let mut item_ids = Vec::with_capacity(uploads.len());
        for upload in uploads {
            let mailbox_id = parse_transfer_mailbox_id(&upload.parent_folder_id)?;
            let mailbox = mailboxes.iter().find(|mailbox| mailbox.id == mailbox_id)
                .ok_or_else(|| anyhow!("UploadItems target mailbox is not writable"))?;
            if matches!(mailbox.role.as_str(), "sent" | "trash" | "junk") {
                bail!("UploadItems target mailbox role is not supported");
            }
            let raw_message = BASE64.decode(upload.data.as_bytes())
                .context("UploadItems Data is not valid base64")?;
            let validation = self.validator.validate_bytes(
                ValidationRequest {
                    ingress_context: IngressContext::JmapEmailImport,
                    declared_mime: Some("message/rfc822".to_string()),
                    filename: None,
                    expected_kind: ExpectedKind::Rfc822Message,
                },
                &raw_message,
            )?;
            if validation.policy_decision != PolicyDecision::Accept {
                bail!("UploadItems blocked by Magika validation: {}", validation.reason);
            }
            let parsed = lpe_storage::mail::parse_rfc822_message(&raw_message)?;
            for attachment in &parsed.attachments {
                let validation = self.validator.validate_bytes(
                    ValidationRequest {
                        ingress_context: IngressContext::AttachmentParsing,
                        declared_mime: Some(attachment.media_type.clone()),
                        filename: Some(attachment.file_name.clone()),
                        expected_kind: ExpectedKind::Any,
                    },
                    &attachment.blob_bytes,
                )?;
                if validation.policy_decision != PolicyDecision::Accept {
                    bail!("UploadItems attachment '{}' blocked by Magika validation: {}", attachment.file_name, validation.reason);
                }
            }
            let payload_hash = hex_sha256(&raw_message);
            item_ids.push(format!("rfc822:{payload_hash}"));
            inputs.push(JmapImportedEmailInput {
                account_id: principal.account_id,
                submitted_by_account_id: principal.account_id,
                mailbox_id,
                source: "ews-upload-items".to_string(),
                raw_message: Some(raw_message),
                from_display: parsed.from.as_ref().and_then(|from| from.display_name.clone()),
                from_address: parsed.from.map(|from| from.email).unwrap_or_else(|| principal.email.clone()),
                sender_display: None,
                sender_address: None,
                to: parsed.to.into_iter().map(|recipient| SubmittedRecipientInput { address: recipient.email, display_name: recipient.display_name }).collect(),
                cc: parsed.cc.into_iter().map(|recipient| SubmittedRecipientInput { address: recipient.email, display_name: recipient.display_name }).collect(),
                bcc: Vec::new(),
                subject: parsed.subject,
                body_text: parsed.body_text,
                body_html_sanitized: parsed.body_html_sanitized,
                internet_message_id: parsed.message_id,
                mime_blob_ref: format!("ews-transfer:{payload_hash}"),
                size_octets: 0,
                received_at: None,
                thread_id: None,
                attachments: parsed.attachments,
            });
            let last = inputs.last_mut().expect("input was appended");
            last.size_octets = last.raw_message.as_ref().map_or(0, |raw| raw.len() as i64);
        }
        let job = self.store.create_ews_transfer_job(
            principal, "import", &item_ids,
            serde_json::json!({ "operation": "UploadItems", "itemCount": item_ids.len() }),
            AuditEntryInput { actor: principal.email.clone(), action: "ews-upload-items".to_string(), subject: format!("{} items", item_ids.len()) },
        ).await?;
        let mut responses = String::new();
        for input in inputs {
            match self.store.import_jmap_email(input, AuditEntryInput {
                actor: principal.email.clone(), action: "ews-upload-items-entry".to_string(), subject: job.id.to_string(),
            }).await {
                Ok(email) => responses.push_str(&format!("<m:UploadItemsResponseMessage ResponseClass=\"Success\"><m:ResponseCode>NoError</m:ResponseCode><m:ItemId Id=\"message:{}\"/></m:UploadItemsResponseMessage>", email.id)),
                Err(error) => responses.push_str(&format!("<m:UploadItemsResponseMessage ResponseClass=\"Error\"><m:ResponseCode>ErrorInvalidOperation</m:ResponseCode><m:MessageText>{}</m:MessageText></m:UploadItemsResponseMessage>", escape_xml(&error.to_string()))),
            }
        }
        Ok(format!("<m:UploadItemsResponse><m:ResponseMessages>{responses}</m:ResponseMessages></m:UploadItemsResponse>"))
    }

    pub(in crate::service) async fn export_items(&self, principal: &AccountPrincipal, request: &str) -> Result<String> {
        let item_ids = requested_item_ids(request);
        if item_ids.is_empty() || item_ids.len() > MAX_TRANSFER_ITEMS {
            bail!("ExportItems requires between one and {MAX_TRANSFER_ITEMS} canonical ItemId values");
        }
        let message_ids = item_ids.iter().map(|item_id| parse_transfer_message_id(item_id)).collect::<Result<Vec<_>>>()?;
        let exports = self.store.fetch_ews_transfer_exports(principal, &message_ids).await?;
        if exports.len() != message_ids.len() {
            bail!("ExportItems contains an inaccessible or missing item");
        }
        let job = self.store.create_ews_transfer_job(
            principal, "export", &item_ids,
            serde_json::json!({ "operation": "ExportItems", "itemCount": item_ids.len() }),
            AuditEntryInput { actor: principal.email.clone(), action: "ews-export-items".to_string(), subject: format!("{} items", item_ids.len()) },
        ).await?;
        let responses = exports.into_iter().map(|export| format!(
            "<m:ExportItemsResponseMessage ResponseClass=\"Success\"><m:ResponseCode>NoError</m:ResponseCode><m:ItemId Id=\"message:{}\"/><m:Data>{}</m:Data></m:ExportItemsResponseMessage>",
            export.message_id, BASE64.encode(strip_bcc_headers(&export.raw_message))
        )).collect::<String>();
        Ok(format!("<m:ExportItemsResponse><m:ResponseMessages>{responses}</m:ResponseMessages><m:JobId>{}</m:JobId></m:ExportItemsResponse>", job.id))
    }
}

fn parse_transfer_mailbox_id(value: &str) -> Result<Uuid> {
    value.trim().strip_prefix("mailbox:").unwrap_or(value.trim()).parse().context("UploadItems ParentFolderId is not a canonical mailbox id")
}

fn parse_transfer_message_id(value: &str) -> Result<Uuid> {
    value.trim().strip_prefix("message:").unwrap_or(value.trim()).parse().context("ExportItems ItemId is not a canonical message id")
}

fn hex_sha256(bytes: &[u8]) -> String { format!("{:x}", Sha256::digest(bytes)) }

fn strip_bcc_headers(raw_message: &[u8]) -> Vec<u8> {
    let source = String::from_utf8_lossy(raw_message);
    let mut output = String::new();
    let mut skipping = false;
    for line in source.lines() {
        if line.is_empty() { skipping = false; output.push_str("\r\n"); continue; }
        if line.to_ascii_lowercase().starts_with("bcc:") { skipping = true; continue; }
        if skipping && (line.starts_with(' ') || line.starts_with('\t')) { continue; }
        skipping = false;
        output.push_str(line);
        output.push_str("\r\n");
    }
    output.into_bytes()
}
