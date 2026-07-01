use super::*;

impl<S: JmapStore, V: lpe_magika::Detector> JmapService<S, V> {
    pub(crate) async fn handle_upload(
        &self,
        authorization: Option<&str>,
        account_id: &str,
        media_type: &str,
        body: &[u8],
    ) -> Result<Value> {
        let account = self.authenticate(authorization).await?;
        let requested_account = self
            .requested_account_access(&account, Some(account_id))
            .await?;
        let requested_account_id = requested_account.account_id;
        if !requested_account.is_owned && !requested_account.may_write {
            bail!("accountId is read-only");
        }
        if body.len() as u64 > MAX_SIZE_UPLOAD {
            bail!("JMAP upload exceeds maxSizeUpload");
        }
        let outcome = self.validator.validate_bytes(
            ValidationRequest {
                ingress_context: IngressContext::JmapUpload,
                declared_mime: Some(media_type.to_string()),
                filename: None,
                expected_kind: ExpectedKind::Any,
            },
            body,
        )?;
        if outcome.policy_decision != PolicyDecision::Accept {
            bail!(
                "JMAP upload blocked by Magika validation: {}",
                outcome.reason
            );
        }
        let blob = self
            .store
            .save_jmap_upload_blob(requested_account_id, media_type, body)
            .await?;

        Ok(json!({
            "accountId": requested_account_id.to_string(),
            "blobId": blob.id.to_string(),
            "type": blob.media_type,
            "size": blob.octet_size,
        }))
    }

    pub(crate) async fn handle_download(
        &self,
        authorization: Option<&str>,
        account_id: &str,
        blob_id: &str,
    ) -> Result<JmapUploadBlob> {
        let account = self.authenticate(authorization).await?;
        let requested_account = self
            .requested_account_access(&account, Some(account_id))
            .await?;
        self.resolve_download_blob(&requested_account, blob_id)
            .await
    }

    pub(crate) async fn resolve_download_blob(
        &self,
        requested_account: &MailboxAccountAccess,
        blob_id: &str,
    ) -> Result<JmapUploadBlob> {
        self.resolve_download_blob_with_bcc(requested_account, blob_id, false)
            .await
    }

    pub(crate) async fn resolve_download_blob_with_bcc(
        &self,
        requested_account: &MailboxAccountAccess,
        blob_id: &str,
        include_bcc: bool,
    ) -> Result<JmapUploadBlob> {
        let requested_account_id = requested_account.account_id;
        match JmapBlobId::parse(blob_id)? {
            JmapBlobId::Upload(blob_id) => self
                .store
                .fetch_jmap_upload_blob(requested_account_id, blob_id)
                .await?
                .ok_or_else(|| anyhow!("blob not found")),
            JmapBlobId::Message(message_id) => {
                if !include_bcc {
                    if let Some(blob) = self
                        .store
                        .fetch_jmap_message_blob(requested_account_id, message_id)
                        .await?
                    {
                        return Ok(blob);
                    }
                }
                let emails = if include_bcc {
                    self.store
                        .fetch_jmap_emails_with_protected_bcc(requested_account_id, &[message_id])
                        .await?
                } else {
                    self.store
                        .fetch_jmap_emails(requested_account_id, &[message_id])
                        .await?
                };
                let email = emails
                    .into_iter()
                    .next()
                    .ok_or_else(|| anyhow!("blob not found"))?;
                let blob_bytes = message_rfc822_bytes(&email, include_bcc);
                Ok(JmapUploadBlob {
                    id: message_id,
                    account_id: requested_account_id,
                    media_type: "message/rfc822".to_string(),
                    octet_size: blob_bytes.len() as u64,
                    blob_bytes,
                })
            }
            JmapBlobId::CalendarAttachment(file_reference) => self
                .store
                .fetch_calendar_attachment_blob(requested_account_id, &file_reference)
                .await?
                .ok_or_else(|| anyhow!("blob not found")),
            JmapBlobId::Opaque(_) => Err(anyhow!("blob not found")),
        }
    }
}
