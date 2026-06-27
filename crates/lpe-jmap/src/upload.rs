use anyhow::{bail, Result};
use lpe_domain::mail_format::{
    format_mailbox_address, normalize_mime_body, rfc5322_utc_date, sanitize_header_value,
    DisplayNamePolicy,
};
use lpe_magika::ExpectedKind;
use lpe_storage::{JmapEmail, JmapEmailAddress};
use uuid::Uuid;

use crate::parse::parse_uuid;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum JmapBlobId {
    Upload(Uuid),
    Message(Uuid),
    CalendarAttachment(String),
    Opaque(String),
}

pub(crate) fn parse_upload_blob_id(value: &str) -> Result<Uuid> {
    match JmapBlobId::parse(value)? {
        JmapBlobId::Upload(id) => Ok(id),
        JmapBlobId::Message(_) | JmapBlobId::CalendarAttachment(_) | JmapBlobId::Opaque(_) => {
            bail!("blob not found")
        }
    }
}

pub(crate) fn expected_attachment_kind(media_type: &str, file_name: &str) -> ExpectedKind {
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

pub(crate) fn blob_id_for_message(email: &JmapEmail) -> String {
    JmapBlobId::for_message(email).into_response_id()
}

impl JmapBlobId {
    pub(crate) fn parse(value: &str) -> Result<Self> {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            bail!("blobId is required");
        }
        if let Ok(id) = Uuid::parse_str(trimmed) {
            return Ok(Self::Upload(id));
        }
        if let Some(upload_id) = trimmed.strip_prefix("upload:") {
            return Ok(Self::Upload(parse_uuid(upload_id)?));
        }
        if let Some(message_id) = trimmed.strip_prefix("message:") {
            return Ok(Self::Message(parse_uuid(message_id)?));
        }
        if trimmed.starts_with("calendar-attachment:") {
            return Ok(Self::CalendarAttachment(trimmed.to_string()));
        }
        if let Some(message_id) = trimmed.strip_prefix("draft-message:") {
            return Ok(Self::Message(parse_uuid(message_id)?));
        }
        if let Some(message_id) = trimmed.strip_prefix("lpe-ct-inbound:") {
            if let Some((_, message_id)) = message_id.rsplit_once(':') {
                return Ok(Self::Message(parse_uuid(message_id)?));
            }
        }
        Ok(Self::Opaque(trimmed.to_string()))
    }

    pub(crate) fn for_message(email: &JmapEmail) -> Self {
        match email.mime_blob_ref.as_deref() {
            Some(value) if value.trim().starts_with("upload:") => {
                Self::parse(value).unwrap_or_else(|_| Self::Opaque(value.trim().to_string()))
            }
            Some(value) if value.trim().starts_with("message:") => {
                Self::parse(value).unwrap_or_else(|_| Self::Opaque(value.trim().to_string()))
            }
            Some(value) if !value.trim().is_empty() => Self::Opaque(value.trim().to_string()),
            _ => Self::Opaque(format!("message:{}", email.id)),
        }
    }

    pub(crate) fn into_response_id(self) -> String {
        match self {
            Self::Upload(id) => format!("upload:{id}"),
            Self::Message(id) => format!("message:{id}"),
            Self::CalendarAttachment(value) => value,
            Self::Opaque(value) => value,
        }
    }
}

pub(crate) fn message_rfc822_bytes(email: &JmapEmail, include_bcc: bool) -> Vec<u8> {
    let mut message = String::new();
    push_header(
        &mut message,
        "From",
        &header_address(&email.from_address, email.from_display.as_deref()),
    );
    if let Some(sender_address) = email.sender_address.as_deref() {
        push_header(
            &mut message,
            "Sender",
            &header_address(sender_address, email.sender_display.as_deref()),
        );
    }
    push_address_list(&mut message, "To", &email.to);
    push_address_list(&mut message, "Cc", &email.cc);
    if include_bcc {
        push_address_list(&mut message, "Bcc", &email.bcc);
    }
    push_header(&mut message, "Subject", &email.subject);
    if let Some(message_id) = email.internet_message_id.as_deref() {
        push_header(&mut message, "Message-ID", message_id);
    }
    push_header(
        &mut message,
        "Date",
        &rfc5322_utc_date(
            email
                .sent_at
                .as_deref()
                .unwrap_or(email.received_at.as_str()),
        ),
    );
    push_header(&mut message, "MIME-Version", "1.0");

    match (&email.body_text, email.body_html_sanitized.as_deref()) {
        (text, Some(html)) if !text.is_empty() => {
            let boundary = format!("lpe-jmap-{}", email.id);
            push_header(
                &mut message,
                "Content-Type",
                &format!("multipart/alternative; boundary=\"{boundary}\""),
            );
            message.push_str("\r\n");
            push_mime_part(&mut message, &boundary, "text/plain", text);
            push_mime_part(&mut message, &boundary, "text/html", html);
            message.push_str("--");
            message.push_str(&boundary);
            message.push_str("--\r\n");
        }
        (_, Some(html)) => {
            push_header(&mut message, "Content-Type", "text/html; charset=utf-8");
            push_header(&mut message, "Content-Transfer-Encoding", "8bit");
            message.push_str("\r\n");
            message.push_str(&normalize_mime_body(html));
        }
        (text, None) => {
            push_header(&mut message, "Content-Type", "text/plain; charset=utf-8");
            push_header(&mut message, "Content-Transfer-Encoding", "8bit");
            message.push_str("\r\n");
            message.push_str(&normalize_mime_body(text));
        }
    }

    message.into_bytes()
}

fn push_mime_part(message: &mut String, boundary: &str, media_type: &str, body: &str) {
    message.push_str("--");
    message.push_str(boundary);
    message.push_str("\r\n");
    push_header(
        message,
        "Content-Type",
        &format!("{media_type}; charset=utf-8"),
    );
    push_header(message, "Content-Transfer-Encoding", "8bit");
    message.push_str("\r\n");
    message.push_str(&normalize_mime_body(body));
    message.push_str("\r\n");
}

fn push_header(message: &mut String, name: &str, value: &str) {
    message.push_str(name);
    message.push_str(": ");
    message.push_str(&sanitize_header_value(value));
    message.push_str("\r\n");
}

fn push_address_list(message: &mut String, name: &str, addresses: &[JmapEmailAddress]) {
    if addresses.is_empty() {
        return;
    }
    let value = addresses
        .iter()
        .map(|address| header_address(&address.address, address.display_name.as_deref()))
        .collect::<Vec<_>>()
        .join(", ");
    push_header(message, name, &value);
}

fn header_address(address: &str, display_name: Option<&str>) -> String {
    format_mailbox_address(address, display_name, DisplayNamePolicy::Include)
}
