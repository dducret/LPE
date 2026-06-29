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
