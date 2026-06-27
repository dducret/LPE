use super::super::*;

pub(in crate::service) fn render_mime_message(
    email: &JmapEmail,
    attachments: &[ActiveSyncAttachmentContent],
) -> Vec<u8> {
    let mut message = render_mime_header(email, attachments.is_empty());
    if attachments.is_empty() {
        message.push_str(&render_standalone_body_mime(email));
    } else {
        let boundary = mixed_boundary(email);
        message.push_str(&format!("--{boundary}\r\n"));
        message.push_str(&render_body_mime_part(email));
        if !message.ends_with("\r\n") {
            message.push_str("\r\n");
        }
        for attachment in attachments {
            message.push_str(&format!("--{boundary}\r\n"));
            message.push_str(&render_attachment_mime_part(attachment));
        }
        message.push_str(&format!("--{boundary}--\r\n"));
    }
    message.into_bytes()
}

pub(in crate::service) fn render_standalone_body_mime(email: &JmapEmail) -> String {
    if let Some(html) = email.body_html_sanitized.as_deref() {
        let boundary = alternative_boundary(email);
        return format!(
            concat!(
                "--{boundary}\r\n",
                "Content-Type: text/plain; charset=UTF-8\r\n",
                "Content-Transfer-Encoding: 7bit\r\n",
                "\r\n",
                "{text}\r\n",
                "--{boundary}\r\n",
                "Content-Type: text/html; charset=UTF-8\r\n",
                "Content-Transfer-Encoding: 7bit\r\n",
                "\r\n",
                "{html}\r\n",
                "--{boundary}--\r\n"
            ),
            boundary = boundary,
            text = email.body_text,
            html = html,
        );
    }

    email.body_text.clone()
}

pub(in crate::service) fn render_mime_header(
    email: &JmapEmail,
    without_attachments: bool,
) -> String {
    let mut lines = Vec::new();
    lines.push(format!(
        "Date: {}",
        sanitize_header_value(&email.received_at)
    ));
    lines.push(format!(
        "From: {}",
        render_mime_address(email.from_display.as_deref(), email.from_address.as_str())
    ));
    if !email.to.is_empty() {
        lines.push(format!("To: {}", render_mime_recipients(&email.to)));
    }
    if !email.cc.is_empty() {
        lines.push(format!("Cc: {}", render_mime_recipients(&email.cc)));
    }
    if !email.bcc.is_empty() && matches!(email.mailbox_role.as_str(), "drafts" | "sent") {
        lines.push(format!("Bcc: {}", render_mime_recipients(&email.bcc)));
    }
    lines.push(format!(
        "Subject: {}",
        sanitize_header_value(&email.subject)
    ));
    if let Some(message_id) = email.internet_message_id.as_deref() {
        lines.push(format!("Message-Id: {}", sanitize_header_value(message_id)));
    }
    lines.push("MIME-Version: 1.0".to_string());
    let content_type = if without_attachments {
        body_content_type(email)
    } else {
        format!("multipart/mixed; boundary=\"{}\"", mixed_boundary(email))
    };
    lines.push(format!("Content-Type: {content_type}"));
    lines.join("\r\n") + "\r\n\r\n"
}

pub(in crate::service) fn render_body_mime_part(email: &JmapEmail) -> String {
    if let Some(html) = email.body_html_sanitized.as_deref() {
        let boundary = alternative_boundary(email);
        return format!(
            concat!(
                "Content-Type: multipart/alternative; boundary=\"{boundary}\"\r\n",
                "\r\n",
                "--{boundary}\r\n",
                "Content-Type: text/plain; charset=UTF-8\r\n",
                "Content-Transfer-Encoding: 7bit\r\n",
                "\r\n",
                "{text}\r\n",
                "--{boundary}\r\n",
                "Content-Type: text/html; charset=UTF-8\r\n",
                "Content-Transfer-Encoding: 7bit\r\n",
                "\r\n",
                "{html}\r\n",
                "--{boundary}--\r\n"
            ),
            boundary = boundary,
            text = email.body_text,
            html = html,
        );
    }

    format!(
        concat!(
            "Content-Type: text/plain; charset=UTF-8\r\n",
            "Content-Transfer-Encoding: 7bit\r\n",
            "\r\n",
            "{}\r\n"
        ),
        email.body_text,
    )
}

pub(in crate::service) fn render_attachment_mime_part(
    attachment: &ActiveSyncAttachmentContent,
) -> String {
    let file_name = quote_mime_parameter(&attachment.file_name);
    format!(
        concat!(
            "Content-Type: {content_type}; name=\"{file_name}\"\r\n",
            "Content-Transfer-Encoding: base64\r\n",
            "Content-Disposition: attachment; filename=\"{file_name}\"\r\n",
            "\r\n",
            "{body}\r\n"
        ),
        content_type = sanitize_header_value(&attachment.media_type),
        file_name = file_name,
        body = base64_mime_lines(&attachment.blob_bytes),
    )
}

pub(in crate::service) fn body_content_type(email: &JmapEmail) -> String {
    if email.body_html_sanitized.is_some() {
        format!(
            "multipart/alternative; boundary=\"{}\"",
            alternative_boundary(email)
        )
    } else {
        "text/plain; charset=UTF-8".to_string()
    }
}

pub(in crate::service) fn mixed_boundary(email: &JmapEmail) -> String {
    format!("lpe-ews-mixed-{}", email.id.simple())
}

pub(in crate::service) fn alternative_boundary(email: &JmapEmail) -> String {
    format!("lpe-ews-alt-{}", email.id.simple())
}

pub(in crate::service) fn render_mime_recipients(recipients: &[JmapEmailAddress]) -> String {
    recipients
        .iter()
        .map(|recipient| render_mime_address(recipient.display_name.as_deref(), &recipient.address))
        .collect::<Vec<_>>()
        .join(", ")
}

pub(in crate::service) fn render_mime_address(display_name: Option<&str>, address: &str) -> String {
    format_mailbox_address(
        address,
        display_name,
        DisplayNamePolicy::OmitIfEqualsAddress,
    )
}

pub(in crate::service) fn quote_mime_parameter(value: &str) -> String {
    quote_header_parameter(value)
}

pub(in crate::service) fn base64_mime_lines(bytes: &[u8]) -> String {
    bytes
        .chunks(57)
        .map(|chunk| BASE64_STANDARD.encode(chunk))
        .collect::<Vec<_>>()
        .join("\r\n")
}

pub(in crate::service) fn message_attachments_xml(attachments: &[ActiveSyncAttachment]) -> String {
    if attachments.is_empty() {
        return String::new();
    }

    format!(
        "<t:Attachments>{}</t:Attachments>",
        attachments
            .iter()
            .map(file_attachment_reference_xml)
            .collect::<String>()
    )
}

pub(in crate::service) fn file_attachment_reference_xml(
    attachment: &ActiveSyncAttachment,
) -> String {
    format!(
        concat!(
            "<t:FileAttachment>",
            "<t:AttachmentId Id=\"{file_reference}\"/>",
            "<t:Name>{name}</t:Name>",
            "<t:ContentType>{content_type}</t:ContentType>",
            "<t:Size>{size}</t:Size>",
            "<t:IsInline>false</t:IsInline>",
            "</t:FileAttachment>"
        ),
        file_reference = escape_xml(&attachment.file_reference),
        name = escape_xml(&attachment.file_name),
        content_type = escape_xml(&attachment.media_type),
        size = attachment.size_octets,
    )
}

pub(in crate::service) fn file_attachment_content_xml(
    content: &ActiveSyncAttachmentContent,
) -> String {
    format!(
        concat!(
            "<t:FileAttachment>",
            "<t:AttachmentId Id=\"{file_reference}\"/>",
            "<t:Name>{name}</t:Name>",
            "<t:ContentType>{content_type}</t:ContentType>",
            "<t:Size>{size}</t:Size>",
            "<t:IsInline>false</t:IsInline>",
            "<t:Content>{body}</t:Content>",
            "</t:FileAttachment>"
        ),
        file_reference = escape_xml(&content.file_reference),
        name = escape_xml(&content.file_name),
        content_type = escape_xml(&content.media_type),
        size = content.blob_bytes.len(),
        body = BASE64_STANDARD.encode(&content.blob_bytes),
    )
}
