use anyhow::{bail, Result};
use lpe_magika::ExpectedKind;
use lpe_storage::{JmapEmail, JmapEmailAddress};
use uuid::Uuid;

use crate::parse::parse_uuid;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum JmapBlobId {
    Upload(Uuid),
    Message(Uuid),
    Opaque(String),
}

pub(crate) fn parse_upload_blob_id(value: &str) -> Result<Uuid> {
    match JmapBlobId::parse(value)? {
        JmapBlobId::Upload(id) => Ok(id),
        JmapBlobId::Message(_) | JmapBlobId::Opaque(_) => bail!("blob not found"),
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
            message.push_str(&normalize_body(html));
        }
        (text, None) => {
            push_header(&mut message, "Content-Type", "text/plain; charset=utf-8");
            push_header(&mut message, "Content-Transfer-Encoding", "8bit");
            message.push_str("\r\n");
            message.push_str(&normalize_body(text));
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
    message.push_str(&normalize_body(body));
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
    let address = sanitize_header_value(address);
    match display_name
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        Some(display_name) => format!("{} <{}>", quote_display_name(display_name), address),
        None => address,
    }
}

fn quote_display_name(value: &str) -> String {
    let value = sanitize_header_value(value);
    if value
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, ' ' | '.' | '_' | '-'))
    {
        value
    } else {
        format!("\"{}\"", value.replace('\\', "\\\\").replace('"', "\\\""))
    }
}

fn sanitize_header_value(value: &str) -> String {
    value
        .replace(['\r', '\n'], " ")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

fn normalize_body(value: &str) -> String {
    let normalized = value.replace("\r\n", "\n").replace('\r', "\n");
    let mut normalized = normalized.replace('\n', "\r\n");
    if !normalized.ends_with("\r\n") {
        normalized.push_str("\r\n");
    }
    normalized
}

fn rfc5322_utc_date(value: &str) -> String {
    let Some((date, time)) = value.trim_end_matches('Z').split_once('T') else {
        return sanitize_header_value(value);
    };
    let mut date_parts = date.split('-').filter_map(|part| part.parse::<i32>().ok());
    let (Some(year), Some(month), Some(day)) =
        (date_parts.next(), date_parts.next(), date_parts.next())
    else {
        return sanitize_header_value(value);
    };
    let weekday = weekday_name(year, month, day);
    let month_name = month_name(month);
    format!("{weekday}, {day:02} {month_name} {year:04} {time} +0000")
}

fn weekday_name(year: i32, month: i32, day: i32) -> &'static str {
    let days = days_from_civil(year, month, day);
    match (days + 4).rem_euclid(7) {
        0 => "Sun",
        1 => "Mon",
        2 => "Tue",
        3 => "Wed",
        4 => "Thu",
        5 => "Fri",
        _ => "Sat",
    }
}

fn month_name(month: i32) -> &'static str {
    match month {
        1 => "Jan",
        2 => "Feb",
        3 => "Mar",
        4 => "Apr",
        5 => "May",
        6 => "Jun",
        7 => "Jul",
        8 => "Aug",
        9 => "Sep",
        10 => "Oct",
        11 => "Nov",
        _ => "Dec",
    }
}

fn days_from_civil(year: i32, month: i32, day: i32) -> i32 {
    let year = year - i32::from(month <= 2);
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let yoe = year - era * 400;
    let month_prime = month + if month > 2 { -3 } else { 9 };
    let doy = (153 * month_prime + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}
