use super::super::*;

impl<S, V> ExchangeService<S, V>
where
    S: ExchangeStore + Clone + Send + Sync + 'static,
    V: Detector + Clone + Send + Sync + 'static,
{
    pub(in crate::service) async fn get_attachment(
        &self,
        principal: &AccountPrincipal,
        request: &str,
    ) -> Result<String> {
        let ids = requested_attachment_ids(request);
        if ids.is_empty() {
            return Ok(operation_error_response(
                "GetAttachment",
                "ErrorInvalidOperation",
                "GetAttachment requires at least one AttachmentId.",
            ));
        }

        let mut attachments = String::new();
        for id in ids {
            let Some(content) = self
                .store
                .fetch_attachment_content(principal.account_id, &id)
                .await?
            else {
                return Ok(operation_error_response(
                    "GetAttachment",
                    "ErrorAttachmentNotFound",
                    "The requested attachment was not found or is not exposed by EWS.",
                ));
            };
            attachments.push_str(&file_attachment_content_xml(&content));
        }

        Ok(get_attachment_success_response(attachments))
    }

    pub(in crate::service) async fn create_attachment(
        &self,
        principal: &AccountPrincipal,
        request: &str,
    ) -> Result<String> {
        let ids = requested_item_ids(request);
        let message_ids = ids
            .iter()
            .filter_map(|id| id.strip_prefix("message:"))
            .map(Uuid::parse_str)
            .collect::<std::result::Result<Vec<_>, _>>()?;
        if ids.len() != 1 || message_ids.len() != 1 {
            return Ok(operation_error_response(
                "CreateAttachment",
                "ErrorInvalidOperation",
                "CreateAttachment currently supports exactly one canonical message parent id.",
            ));
        }
        if element_content(request, "ItemAttachment").is_some() {
            return Ok(operation_error_response(
                "CreateAttachment",
                "ErrorInvalidOperation",
                "CreateAttachment currently supports only FileAttachment payloads.",
            ));
        }

        let file_attachments = element_contents(request, "FileAttachment");
        if file_attachments.is_empty() {
            return Ok(operation_error_response(
                "CreateAttachment",
                "ErrorInvalidOperation",
                "CreateAttachment requires at least one FileAttachment.",
            ));
        }

        let message_id = message_ids[0];
        let mut attachments = String::new();
        let mut root_item = String::new();
        for file_attachment in file_attachments {
            let mut attachment = match parse_file_attachment_upload(file_attachment) {
                Ok(attachment) => attachment,
                Err(error) => {
                    return Ok(operation_error_response(
                        "CreateAttachment",
                        "ErrorInvalidOperation",
                        &error.to_string(),
                    ));
                }
            };

            let declared_mime = element_text(file_attachment, "ContentType")
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty());
            let outcome = self.validator.validate_bytes(
                ValidationRequest {
                    ingress_context: IngressContext::ExchangeAttachment,
                    declared_mime: declared_mime.clone(),
                    filename: Some(attachment.file_name.clone()),
                    expected_kind: expected_attachment_kind(
                        &attachment.media_type,
                        &attachment.file_name,
                    ),
                },
                &attachment.blob_bytes,
            )?;
            if outcome.policy_decision != PolicyDecision::Accept {
                return Ok(operation_error_response(
                    "CreateAttachment",
                    "ErrorInvalidOperation",
                    &outcome.reason,
                ));
            }
            if declared_mime.is_none() && !outcome.detected_mime.trim().is_empty() {
                attachment.media_type = outcome.detected_mime.clone();
            }

            let Some((email, stored_attachment)) = self
                .store
                .add_message_attachment(
                    principal.account_id,
                    message_id,
                    attachment,
                    AuditEntryInput {
                        actor: principal.email.clone(),
                        action: "ews-create-attachment".to_string(),
                        subject: format!("message:{message_id}"),
                    },
                )
                .await?
            else {
                return Ok(operation_error_response(
                    "CreateAttachment",
                    "ErrorItemNotFound",
                    "The requested parent message was not found or is not exposed by EWS.",
                ));
            };
            root_item = root_item_id_xml(&email);
            attachments.push_str(&file_attachment_reference_xml(&stored_attachment));
        }

        Ok(create_attachment_success_response(attachments, root_item))
    }

    pub(in crate::service) async fn delete_attachment(
        &self,
        principal: &AccountPrincipal,
        request: &str,
    ) -> Result<String> {
        let ids = requested_attachment_ids(request);
        if ids.is_empty() {
            return Ok(operation_error_response(
                "DeleteAttachment",
                "ErrorInvalidOperation",
                "DeleteAttachment requires at least one AttachmentId.",
            ));
        }

        let mut root_items = String::new();
        for id in ids {
            let Some(email) = self
                .store
                .delete_message_attachment(
                    principal.account_id,
                    &id,
                    AuditEntryInput {
                        actor: principal.email.clone(),
                        action: "ews-delete-attachment".to_string(),
                        subject: id.clone(),
                    },
                )
                .await?
            else {
                return Ok(operation_error_response(
                    "DeleteAttachment",
                    "ErrorAttachmentNotFound",
                    "The requested attachment was not found or is not exposed by EWS.",
                ));
            };
            root_items.push_str(&root_item_id_xml(&email));
        }

        Ok(delete_attachment_success_response(root_items))
    }
}

pub(in crate::service) fn get_attachment_success_response(attachments: String) -> String {
    format!(
        concat!(
            "<m:GetAttachmentResponse>",
            "<m:ResponseMessages>",
            "<m:GetAttachmentResponseMessage ResponseClass=\"Success\">",
            "<m:ResponseCode>NoError</m:ResponseCode>",
            "<m:Attachments>{attachments}</m:Attachments>",
            "</m:GetAttachmentResponseMessage>",
            "</m:ResponseMessages>",
            "</m:GetAttachmentResponse>"
        ),
        attachments = attachments,
    )
}

pub(in crate::service) fn create_attachment_success_response(
    attachments: String,
    root_item: String,
) -> String {
    format!(
        concat!(
            "<m:CreateAttachmentResponse>",
            "<m:ResponseMessages>",
            "<m:CreateAttachmentResponseMessage ResponseClass=\"Success\">",
            "<m:ResponseCode>NoError</m:ResponseCode>",
            "<m:Attachments>{attachments}</m:Attachments>",
            "{root_item}",
            "</m:CreateAttachmentResponseMessage>",
            "</m:ResponseMessages>",
            "</m:CreateAttachmentResponse>"
        ),
        attachments = attachments,
        root_item = root_item,
    )
}

pub(in crate::service) fn delete_attachment_success_response(root_items: String) -> String {
    format!(
        concat!(
            "<m:DeleteAttachmentResponse>",
            "<m:ResponseMessages>",
            "<m:DeleteAttachmentResponseMessage ResponseClass=\"Success\">",
            "<m:ResponseCode>NoError</m:ResponseCode>",
            "{root_items}",
            "</m:DeleteAttachmentResponseMessage>",
            "</m:ResponseMessages>",
            "</m:DeleteAttachmentResponse>"
        ),
        root_items = root_items,
    )
}

pub(in crate::service) fn parse_file_attachment_upload(
    value: &str,
) -> Result<AttachmentUploadInput> {
    let file_name = element_text(value, "Name")
        .map(|name| name.trim().to_string())
        .filter(|name| !name.is_empty())
        .ok_or_else(|| anyhow!("FileAttachment Name is required"))?;
    let media_type = element_text(value, "ContentType")
        .map(|content_type| content_type.trim().to_string())
        .filter(|content_type| !content_type.is_empty())
        .unwrap_or_else(|| "application/octet-stream".to_string());
    let content = element_text(value, "Content")
        .map(|content| content.trim().to_string())
        .filter(|content| !content.is_empty())
        .ok_or_else(|| anyhow!("FileAttachment Content is required"))?;
    let blob_bytes = BASE64_STANDARD
        .decode(content.as_bytes())
        .map_err(|_| anyhow!("FileAttachment Content must be valid base64"))?;

    Ok(AttachmentUploadInput {
        file_name,
        media_type,
        disposition: Some("attachment".to_string()),
        content_id: None,
        blob_bytes,
    })
}

pub(in crate::service) fn expected_attachment_kind(
    media_type: &str,
    file_name: &str,
) -> ExpectedKind {
    let media_type = media_type.trim().to_ascii_lowercase();
    let file_name = file_name.trim().to_ascii_lowercase();
    if matches!(
        media_type.as_str(),
        "application/pdf"
            | "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
            | "application/vnd.oasis.opendocument.text"
    ) || file_name.ends_with(".pdf")
        || file_name.ends_with(".docx")
        || file_name.ends_with(".odt")
    {
        ExpectedKind::SupportedAttachmentText
    } else {
        ExpectedKind::Any
    }
}
