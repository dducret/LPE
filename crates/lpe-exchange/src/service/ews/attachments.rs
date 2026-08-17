use super::super::*;

enum AttachmentReference {
    Message(String),
    Calendar { file_reference: String, event_id: Uuid },
}

struct ParsedFileAttachment {
    upload: AttachmentUploadInput,
    declared_mime: Option<String>,
}

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
        let id = parse_attachment_reference(request, "GetAttachment")?;
        let file_reference = attachment_reference_value(&id);
        let content_account_id = match &id {
            AttachmentReference::Message(_) => principal.account_id,
            AttachmentReference::Calendar { event_id, .. } => {
                let Some(event) = self
                    .store
                    .fetch_accessible_events_by_ids(principal.account_id, &[*event_id])
                    .await?
                    .into_iter()
                    .find(|event| event.id == *event_id && event.rights.may_read)
                else {
                    return Ok(operation_error_response(
                        "GetAttachment",
                        "ErrorAttachmentNotFound",
                        "The requested attachment was not found or is not exposed by EWS.",
                    ));
                };
                event.owner_account_id
            }
        };
        let Some(content) = self
            .store
            .fetch_attachment_content(content_account_id, file_reference)
            .await?
        else {
            return Ok(operation_error_response(
                "GetAttachment",
                "ErrorAttachmentNotFound",
                "The requested attachment was not found or is not exposed by EWS.",
            ));
        };

        Ok(get_attachment_success_response(
            file_attachment_content_xml(&content),
        ))
    }

    pub(in crate::service) async fn create_attachment(
        &self,
        principal: &AccountPrincipal,
        request: &str,
    ) -> Result<String> {
        let (message_id, mut attachment) = parse_create_file_attachment(request)?;
        if let Err(error) = self
            .validate_mutating_item_change_keys(principal, request)
            .await
        {
            return Ok(operation_error_response(
                "CreateAttachment",
                ews_error_code_or(&error, "ErrorInvalidOperation"),
                &error.to_string(),
            ));
        }
        if self
            .store
            .fetch_jmap_emails(principal.account_id, &[message_id])
            .await?
            .is_empty()
        {
            return Ok(operation_error_response(
                "CreateAttachment",
                "ErrorItemNotFound",
                "The requested parent message was not found or is not exposed by EWS.",
            ));
        }

        let declared_mime = attachment.declared_mime.take();
        let outcome = self.validator.validate_bytes(
            ValidationRequest {
                ingress_context: IngressContext::ExchangeAttachment,
                declared_mime: declared_mime.clone(),
                filename: Some(attachment.upload.file_name.clone()),
                expected_kind: expected_attachment_kind(
                    &attachment.upload.media_type,
                    &attachment.upload.file_name,
                ),
            },
            &attachment.upload.blob_bytes,
        )?;
        if outcome.policy_decision != PolicyDecision::Accept {
            return Ok(operation_error_response(
                "CreateAttachment",
                "ErrorInvalidOperation",
                &outcome.reason,
            ));
        }
        if declared_mime.is_none() && !outcome.detected_mime.trim().is_empty() {
            attachment.upload.media_type = outcome.detected_mime;
        }

        let Some((email, stored_attachment)) = self
            .store
            .add_message_attachment(
                principal.account_id,
                message_id,
                attachment.upload,
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

        Ok(create_attachment_success_response(
            file_attachment_reference_xml(&stored_attachment),
            root_item_id_xml(&email),
        ))
    }

    pub(in crate::service) async fn delete_attachment(
        &self,
        principal: &AccountPrincipal,
        request: &str,
    ) -> Result<String> {
        let id = parse_attachment_reference(request, "DeleteAttachment")?;
        let root_item = match id {
            AttachmentReference::Message(file_reference) => {
                let Some(email) = self
                    .store
                    .delete_message_attachment(
                        principal.account_id,
                        &file_reference,
                        AuditEntryInput {
                            actor: principal.email.clone(),
                            action: "ews-delete-attachment".to_string(),
                            subject: file_reference.clone(),
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
                root_item_id_xml(&email)
            }
            AttachmentReference::Calendar {
                file_reference,
                event_id,
            } => {
                let Some(event) = self
                    .store
                    .fetch_accessible_events_by_ids(principal.account_id, &[event_id])
                    .await?
                    .into_iter()
                    .find(|event| event.id == event_id && event.rights.may_delete)
                else {
                    return Ok(operation_error_response(
                        "DeleteAttachment",
                        "ErrorAttachmentNotFound",
                        "The requested attachment was not found or is not exposed by EWS.",
                    ));
                };
                let Some(event_id) = self
                    .store
                    .delete_calendar_event_attachment(
                        event.owner_account_id,
                        &file_reference,
                        AuditEntryInput {
                            actor: principal.email.clone(),
                            action: "ews-delete-calendar-attachment".to_string(),
                            subject: file_reference.clone(),
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
                let event = self
                    .store
                    .fetch_accessible_events_by_ids(principal.account_id, &[event_id])
                    .await?
                    .into_iter()
                    .next()
                    .ok_or_else(|| anyhow!("calendar attachment parent was not found"))?;
                let change_keys = event_change_keys(
                    &self.store,
                    principal.account_id,
                    std::slice::from_ref(&event),
                )
                .await?;
                format!(
                    "<m:RootItemId RootItemId=\"event:{id}\" RootItemChangeKey=\"{change_key}\"/>",
                    id = event.id,
                    change_key = escape_xml(&change_key_for(&change_keys, event.id, "calendar")?),
                )
            }
        };

        Ok(delete_attachment_success_response(root_item))
    }
}

/// [MS-OXWSATT] sections 2.2.4.2, 2.2.4.5, and 3.1.4.1: LPE accepts one
/// canonical message parent and one FileAttachment, so all validation finishes
/// before its single canonical mutation.
fn parse_create_file_attachment(request: &str) -> Result<(Uuid, ParsedFileAttachment)> {
    let parents = element_contents(request, "ParentItemId");
    if parents.len() != 1 {
        bail!("CreateAttachment requires exactly one ParentItemId");
    }
    let parent_ids = attribute_values_for_tag(parents[0], "ItemId", "Id");
    if parent_ids.len() != 1 {
        bail!("CreateAttachment requires exactly one canonical message parent id");
    }
    let message_id = parent_ids[0]
        .strip_prefix("message:")
        .ok_or_else(|| anyhow!("CreateAttachment parent item is not a canonical message"))
        .and_then(|value| {
            Uuid::parse_str(value)
                .map_err(|_| anyhow!("CreateAttachment parent message id is invalid"))
        })?;

    let attachments = element_contents(request, "Attachments");
    if attachments.len() != 1 {
        bail!("CreateAttachment requires exactly one Attachments payload");
    }
    let attachments = attachments[0];
    if !element_contents(attachments, "ItemAttachment").is_empty()
        || !element_contents(attachments, "ReferenceAttachment").is_empty()
    {
        bail!("CreateAttachment currently supports only FileAttachment payloads");
    }
    let file_attachments = element_contents(attachments, "FileAttachment");
    if file_attachments.len() != 1 {
        bail!("CreateAttachment supports exactly one FileAttachment per request");
    }
    let file_attachment = file_attachments[0];
    let declared_mime = element_text(file_attachment, "ContentType")
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());

    Ok((
        message_id,
        ParsedFileAttachment {
            upload: parse_file_attachment_upload(file_attachment)?,
            declared_mime,
        },
    ))
}

/// [MS-OXWSATT] sections 2.2.4.2, 3.1.4.2, and 3.1.4.3: attachment reads
/// and deletes use a single well-formed canonical LPE attachment reference.
fn parse_attachment_reference(request: &str, operation: &str) -> Result<AttachmentReference> {
    let attachment_ids = element_contents(request, "AttachmentIds");
    if attachment_ids.len() != 1 {
        bail!("{operation} requires exactly one AttachmentIds payload");
    }
    let ids = attribute_values_for_tag(attachment_ids[0], "AttachmentId", "Id");
    if ids.len() != 1 {
        bail!("{operation} requires exactly one AttachmentId");
    }
    parse_canonical_attachment_reference(ids[0], operation)
}

fn parse_canonical_attachment_reference(
    value: &str,
    operation: &str,
) -> Result<AttachmentReference> {
    let mut parts = value.trim().split(':');
    let kind = parts.next();
    let parent_id = parts.next();
    let attachment_id = parts.next();
    if parts.next().is_some() {
        bail!("{operation} attachment id is invalid");
    }
    let parent_id = parent_id
        .ok_or_else(|| anyhow!("{operation} attachment id is invalid"))
        .and_then(|id| {
            Uuid::parse_str(id).map_err(|_| anyhow!("{operation} attachment id is invalid"))
        })?;
    let attachment_id = attachment_id
        .ok_or_else(|| anyhow!("{operation} attachment id is invalid"))
        .and_then(|id| {
            Uuid::parse_str(id).map_err(|_| anyhow!("{operation} attachment id is invalid"))
        })?;
    match kind {
        Some("attachment") => Ok(AttachmentReference::Message(format!(
            "attachment:{parent_id}:{attachment_id}"
        ))),
        Some("calendar-attachment") => Ok(AttachmentReference::Calendar {
            file_reference: format!("calendar-attachment:{parent_id}:{attachment_id}"),
            event_id: parent_id,
        }),
        _ => bail!("{operation} attachment id is not supported"),
    }
}

fn attachment_reference_value(reference: &AttachmentReference) -> &str {
    match reference {
        AttachmentReference::Message(value) => value,
        AttachmentReference::Calendar { file_reference, .. } => file_reference,
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
    let disposition = match element_text(value, "IsInline")
        .map(|value| value.trim().to_ascii_lowercase())
        .as_deref()
    {
        None | Some("false") => "attachment",
        Some("true") => "inline",
        Some(_) => bail!("FileAttachment IsInline must be true or false"),
    };
    let content_id = element_text(value, "ContentId")
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());
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
        disposition: Some(disposition.to_string()),
        content_id,
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
