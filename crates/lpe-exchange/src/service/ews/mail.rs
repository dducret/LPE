use super::super::*;

pub(in crate::service) fn message_summary_xml(email: &JmapEmail) -> String {
    format!(
        concat!(
            "<t:Message>",
            "<t:ItemId Id=\"message:{id}\" ChangeKey=\"{change_key}\"/>",
            "<t:ParentFolderId Id=\"mailbox:{mailbox_id}\"/>",
            "<t:Subject>{subject}</t:Subject>",
            "<t:DateTimeReceived>{received_at}</t:DateTimeReceived>",
            "<t:Size>{size}</t:Size>",
            "<t:HasAttachments>{has_attachments}</t:HasAttachments>",
            "<t:IsRead>{is_read}</t:IsRead>",
            "</t:Message>"
        ),
        id = email.id,
        change_key = escape_xml(&email.delivery_status),
        mailbox_id = email.mailbox_id,
        subject = escape_xml(&email.subject),
        received_at = escape_xml(&email.received_at),
        size = email.size_octets.max(0),
        has_attachments = email.has_attachments,
        is_read = !email.unread,
    )
}

pub(in crate::service) fn message_item_xml(email: &JmapEmail) -> String {
    message_item_xml_with_attachments(email, &[])
}

fn message_item_xml_with_attachments(
    email: &JmapEmail,
    attachments: &[ActiveSyncAttachment],
) -> String {
    message_item_xml_with_details(email, attachments, None)
}

pub(in crate::service) fn message_item_xml_with_details(
    email: &JmapEmail,
    attachments: &[ActiveSyncAttachment],
    mime_attachment_contents: Option<&[ActiveSyncAttachmentContent]>,
) -> String {
    let mut xml = message_summary_xml(email);
    let mime_content = mime_attachment_contents
        .map(|contents| {
            format!(
                "<t:MimeContent CharacterSet=\"UTF-8\">{}</t:MimeContent>",
                BASE64_STANDARD.encode(render_mime_message(email, contents))
            )
        })
        .unwrap_or_default();
    xml.insert_str(
        xml.len() - "</t:Message>".len(),
        &format!(
            "{}<t:Body BodyType=\"Text\">{}</t:Body>{}",
            mime_content,
            escape_xml(&email.body_text),
            message_attachments_xml(attachments),
        ),
    );
    xml
}

pub(in crate::service) fn root_item_id_xml(email: &JmapEmail) -> String {
    format!(
        "<m:RootItemId RootItemId=\"message:{id}\" RootItemChangeKey=\"{change_key}\"/>",
        id = email.id,
        change_key = escape_xml(&email.delivery_status),
    )
}

pub(in crate::service) fn requested_mime_content(request: &str) -> bool {
    request.contains("item:MimeContent") || request.contains("MimeContent")
}

impl<S, V> ExchangeService<S, V>
where
    S: ExchangeStore + Clone + Send + Sync + 'static,
    V: Detector + Clone + Send + Sync + 'static,
{
    pub(in crate::service) async fn mark_as_junk(
        &self,
        principal: &AccountPrincipal,
        request: &str,
    ) -> Result<String> {
        let result = async {
            let is_junk = ews_bool_attribute(request, "MarkAsJunk", "IsJunk").unwrap_or(true);
            let move_item = ews_bool_attribute(request, "MarkAsJunk", "MoveItem").unwrap_or(false);
            if !is_junk || !move_item {
                bail!(
                    "LPE supports MarkAsJunk only for moving messages to the canonical Junk mailbox; Exchange blocked-sender and unblock state is not protocol-local state."
                );
            }
            let ids = requested_item_ids(request);
            let message_ids = ids
                .iter()
                .filter_map(|id| id.strip_prefix("message:"))
                .map(Uuid::parse_str)
                .collect::<std::result::Result<Vec<_>, _>>()?;
            if ids.is_empty() || message_ids.len() != ids.len() {
                bail!("MarkAsJunk currently supports only canonical message item ids.");
            }
            let mailboxes = self
                .store
                .fetch_jmap_mailboxes(principal.account_id)
                .await?;
            let Some(junk_mailbox_id) = mailboxes
                .iter()
                .find(|mailbox| mailbox.role == "junk")
                .map(|mailbox| mailbox.id)
            else {
                return Ok(operation_error_response(
                    "MarkAsJunk",
                    "ErrorFolderNotFound",
                    "The canonical Junk mailbox was not found.",
                ));
            };

            let existing = self
                .store
                .fetch_jmap_emails(principal.account_id, &message_ids)
                .await?;
            if existing.len() != message_ids.len() {
                return Ok(operation_error_response(
                    "MarkAsJunk",
                    "ErrorItemNotFound",
                    "message not found",
                ));
            }

            let mut moved_item_ids = String::new();
            for message_id in message_ids {
                let moved = self
                    .store
                    .move_jmap_email(
                        principal.account_id,
                        message_id,
                        junk_mailbox_id,
                        AuditEntryInput {
                            actor: principal.email.clone(),
                            action: "ews-mark-as-junk".to_string(),
                            subject: format!("{message_id}->{junk_mailbox_id}"),
                        },
                    )
                    .await?;
                moved_item_ids.push_str(&format!(
                    "<m:MovedItemId Id=\"message:{}\" ChangeKey=\"{}\"/>",
                    moved.id, moved.modseq
                ));
            }
            Ok(mark_as_junk_success_response(moved_item_ids))
        }
        .await;

        Ok(result.unwrap_or_else(|error: anyhow::Error| {
            operation_error_response("MarkAsJunk", "ErrorInvalidOperation", &error.to_string())
        }))
    }
}

pub(in crate::service) fn create_item_success_response(
    message_id: Uuid,
    delivery_status: &str,
) -> String {
    format!(
        concat!(
            "<m:CreateItemResponse>",
            "<m:ResponseMessages>",
            "<m:CreateItemResponseMessage ResponseClass=\"Success\">",
            "<m:ResponseCode>NoError</m:ResponseCode>",
            "<m:Items>",
            "<t:Message>",
            "<t:ItemId Id=\"message:{message_id}\" ChangeKey=\"{delivery_status}\"/>",
            "</t:Message>",
            "</m:Items>",
            "</m:CreateItemResponseMessage>",
            "</m:ResponseMessages>",
            "</m:CreateItemResponse>"
        ),
        message_id = message_id,
        delivery_status = escape_xml(delivery_status),
    )
}

pub(in crate::service) fn parse_create_message_input(
    principal: &AccountPrincipal,
    request: &str,
) -> Result<SubmitMessageInput> {
    let message = element_content(request, "Message")
        .ok_or_else(|| anyhow!("CreateItem currently supports only Message items"))?;
    let body_tag = open_tag_text(message, "Body").unwrap_or_default();
    let body_type = attribute_value(body_tag, "BodyType").unwrap_or("Text");
    let body_value = element_text(message, "Body").unwrap_or_default();
    let body_text = if body_type.eq_ignore_ascii_case("HTML") {
        html_to_text(&body_value)
    } else {
        body_value.clone()
    };
    let from = element_content(message, "From").and_then(parse_first_mailbox);
    let sender = element_content(message, "Sender").and_then(parse_first_mailbox);
    let from_display = from
        .as_ref()
        .and_then(|mailbox| mailbox.display_name.clone())
        .or_else(|| Some(principal.display_name.clone()));
    let from_address = from
        .map(|mailbox| mailbox.address)
        .unwrap_or_else(|| principal.email.clone());

    Ok(SubmitMessageInput {
        draft_message_id: None,
        account_id: principal.account_id,
        submitted_by_account_id: principal.account_id,
        source: "ews-createitem".to_string(),
        from_display,
        from_address,
        sender_display: sender
            .as_ref()
            .and_then(|mailbox| mailbox.display_name.clone()),
        sender_address: sender.map(|mailbox| mailbox.address),
        to: parse_recipients(message, "ToRecipients"),
        cc: parse_recipients(message, "CcRecipients"),
        bcc: parse_recipients(message, "BccRecipients"),
        subject: element_text(message, "Subject").unwrap_or_default(),
        body_text,
        body_html_sanitized: None,
        internet_message_id: element_text(message, "InternetMessageId"),
        mime_blob_ref: Some(format!("ews-createitem:{}", Uuid::new_v4())),
        size_octets: message.len() as i64,
        unread: Some(false),
        flagged: Some(false),
        attachments: Vec::new(),
    })
}

pub(in crate::service) fn imported_email_input(
    input: SubmitMessageInput,
    mailbox_id: Uuid,
) -> JmapImportedEmailInput {
    JmapImportedEmailInput {
        account_id: input.account_id,
        submitted_by_account_id: input.submitted_by_account_id,
        mailbox_id,
        source: input.source,
        raw_message: None,
        from_display: input.from_display,
        from_address: input.from_address,
        sender_display: input.sender_display,
        sender_address: input.sender_address,
        to: input.to,
        cc: input.cc,
        bcc: input.bcc,
        subject: input.subject,
        body_text: input.body_text,
        body_html_sanitized: input.body_html_sanitized,
        internet_message_id: input.internet_message_id,
        mime_blob_ref: input
            .mime_blob_ref
            .unwrap_or_else(|| format!("ews-createitem:{}", Uuid::new_v4())),
        size_octets: input.size_octets,
        received_at: None,
        thread_id: None,
        attachments: input.attachments,
    }
}

pub(in crate::service) fn parse_update_message_flags(
    request: &str,
) -> Result<Option<(Option<bool>, Option<bool>)>> {
    let unread = element_text(request, "IsRead")
        .map(|value| parse_xml_bool(&value).map(|is_read| !is_read))
        .transpose()?;
    let mut flagged = element_text(request, "FlagStatus")
        .map(|value| match value.trim().to_ascii_lowercase().as_str() {
            "notflagged" => Ok(false),
            "flagged" | "complete" => Ok(true),
            other => bail!("unsupported message FlagStatus {other}"),
        })
        .transpose()?;
    if field_deleted(request, "message:Flag") || field_deleted(request, "message:FlagStatus") {
        flagged = Some(false);
    }

    Ok((unread.is_some() || flagged.is_some()).then_some((unread, flagged)))
}
