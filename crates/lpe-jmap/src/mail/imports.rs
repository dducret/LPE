use anyhow::{anyhow, bail, Result};
use lpe_magika::{ExpectedKind, IngressContext, PolicyDecision, ValidationRequest};
use lpe_storage::{
    mail::parse_rfc822_message, AuthenticatedAccount, JmapImportedEmailInput, MailboxAccountAccess,
};
use serde_json::Value;
use std::collections::HashMap;
use uuid::Uuid;

use crate::{
    convert::map_parsed_recipients, parse::parse_uuid, upload::parse_upload_blob_id, JmapService,
};

impl<S: crate::store::JmapStore, V: lpe_magika::Detector> JmapService<S, V> {
    pub(crate) async fn parse_email_import(
        &self,
        account: &AuthenticatedAccount,
        account_access: &MailboxAccountAccess,
        value: Value,
        created_ids: &HashMap<String, String>,
    ) -> Result<JmapImportedEmailInput> {
        let object = value
            .as_object()
            .ok_or_else(|| anyhow!("import arguments must be an object"))?;
        let blob_id = object
            .get("blobId")
            .and_then(Value::as_str)
            .map(|value| crate::resolve_creation_reference(value, created_ids))
            .ok_or_else(|| anyhow!("blobId is required"))?;
        let blob_id = parse_upload_blob_id(&blob_id)?;
        let mailbox_ids = object
            .get("mailboxIds")
            .and_then(Value::as_object)
            .ok_or_else(|| anyhow!("mailboxIds is required"))?;
        let target_mailbox_id = mailbox_ids
            .iter()
            .find(|(_, included)| included.as_bool().unwrap_or(false))
            .map(|(mailbox_id, _)| parse_uuid(mailbox_id))
            .transpose()?
            .ok_or_else(|| anyhow!("one target mailboxId is required"))?;
        self.ensure_target_mailbox_accepts_message_write(
            account_access.account_id,
            target_mailbox_id,
            account_access,
        )
        .await?;
        let blob = self
            .store
            .fetch_jmap_upload_blob(account_access.account_id, blob_id)
            .await?
            .ok_or_else(|| anyhow!("uploaded blob not found"))?;
        let outcome = self.validator.validate_bytes(
            ValidationRequest {
                ingress_context: IngressContext::JmapEmailImport,
                declared_mime: Some(blob.media_type.clone()),
                filename: None,
                expected_kind: ExpectedKind::Rfc822Message,
            },
            &blob.blob_bytes,
        )?;
        if outcome.policy_decision != PolicyDecision::Accept {
            bail!(
                "JMAP email import blocked by Magika validation: {}",
                outcome.reason
            );
        }
        let parsed = parse_rfc822_message(&blob.blob_bytes)?;
        self.validate_imported_attachments(&parsed.attachments)?;

        Ok(JmapImportedEmailInput {
            account_id: account_access.account_id,
            submitted_by_account_id: account.account_id,
            mailbox_id: target_mailbox_id,
            source: "jmap-import".to_string(),
            raw_message: Some(blob.blob_bytes),
            from_display: parsed
                .from
                .as_ref()
                .and_then(|from| from.display_name.clone())
                .or(Some(account_access.display_name.clone())),
            from_address: parsed
                .from
                .map(|from| from.email)
                .unwrap_or_else(|| account_access.email.clone()),
            sender_display: None,
            sender_address: None,
            to: map_parsed_recipients(parsed.to),
            cc: map_parsed_recipients(parsed.cc),
            bcc: Vec::new(),
            subject: parsed.subject,
            body_text: parsed.body_text,
            body_html_sanitized: parsed.body_html_sanitized,
            internet_message_id: parsed.message_id,
            mime_blob_ref: format!("upload:{}", blob.id),
            size_octets: blob.octet_size as i64,
            received_at: None,
            thread_id: None,
            attachments: parsed.attachments,
        })
    }

    pub(crate) async fn ensure_target_mailbox_accepts_message_write(
        &self,
        account_id: Uuid,
        target_mailbox_id: Uuid,
        account_access: &MailboxAccountAccess,
    ) -> Result<()> {
        crate::mailboxes::ensure_mailbox_write(crate::mailboxes::mailbox_account_may_write(
            account_access,
        ))?;
        if let Some(target_mailbox) = self
            .store
            .fetch_jmap_mailboxes(account_id)
            .await?
            .into_iter()
            .find(|mailbox| mailbox.id == target_mailbox_id)
        {
            if target_mailbox.role == "drafts" {
                crate::mailboxes::ensure_mailbox_draft_write(account_access)?;
            }
        }

        Ok(())
    }
}
