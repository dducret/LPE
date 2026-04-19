use anyhow::Result;
use lpe_magika::collect_mime_attachment_parts;
use std::collections::HashMap;

use crate::{AttachmentUploadInput, SubmittedRecipientInput};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedMailAddress {
    pub email: String,
    pub display_name: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ParsedRfc822Message {
    pub from: Option<ParsedMailAddress>,
    pub to: Vec<ParsedMailAddress>,
    pub cc: Vec<ParsedMailAddress>,
    pub subject: String,
    pub message_id: Option<String>,
    pub body_text: String,
    pub attachments: Vec<AttachmentUploadInput>,
}

pub fn parse_message_attachments(bytes: &[u8]) -> Result<Vec<AttachmentUploadInput>> {
    collect_mime_attachment_parts(bytes)?
        .into_iter()
        .enumerate()
        .map(|(index, attachment)| {
            let file_name = attachment
                .filename
                .unwrap_or_else(|| format!("attachment-{}.bin", index + 1));
            let media_type = attachment
                .declared_mime
                .unwrap_or_else(|| "application/octet-stream".to_string());
            Ok(AttachmentUploadInput {
                file_name,
                media_type,
                blob_bytes: attachment.bytes,
            })
        })
        .collect()
}

pub fn parse_header_recipients(
    raw_message: &[u8],
    header_name: &str,
) -> Vec<SubmittedRecipientInput> {
    let expected = format!("{}:", header_name.to_ascii_lowercase());
    unfolded_headers(raw_message)
        .into_iter()
        .find_map(|line| {
            let lower = line.to_ascii_lowercase();
            if lower.starts_with(&expected) {
                Some(
                    parse_address_list(
                        line.split_once(':')
                            .map(|(_, value)| value)
                            .unwrap_or_default(),
                    )
                    .into_iter()
                    .map(|address| SubmittedRecipientInput {
                        address: address.email,
                        display_name: address.display_name,
                    })
                    .collect(),
                )
            } else {
                None
            }
        })
        .unwrap_or_default()
}

pub fn parse_rfc822_message(bytes: &[u8]) -> Result<ParsedRfc822Message> {
    let raw = String::from_utf8_lossy(bytes).replace("\r\n", "\n");
    let (header_text, body_text) = raw.split_once("\n\n").unwrap_or((raw.as_str(), ""));
    let headers = parse_headers(header_text);

    Ok(ParsedRfc822Message {
        from: headers
            .get("from")
            .and_then(|value| parse_single_address(value)),
        to: headers
            .get("to")
            .map(|value| parse_address_list(value))
            .unwrap_or_default(),
        cc: headers
            .get("cc")
            .map(|value| parse_address_list(value))
            .unwrap_or_default(),
        subject: headers.get("subject").cloned().unwrap_or_default(),
        message_id: headers.get("message-id").cloned(),
        body_text: body_text.trim().to_string(),
        attachments: parse_message_attachments(bytes)?
            .into_iter()
            .map(|mut attachment| {
                trim_trailing_crlf(&mut attachment.blob_bytes);
                attachment
            })
            .collect(),
    })
}

pub fn parse_headers_map(raw_message: &[u8]) -> HashMap<String, String> {
    let raw = String::from_utf8_lossy(raw_message).replace("\r\n", "\n");
    let (header_text, _) = raw.split_once("\n\n").unwrap_or((raw.as_str(), ""));
    parse_headers(header_text)
}

fn normalize_email(value: &str) -> String {
    value.trim().to_lowercase()
}

fn parse_headers(input: &str) -> HashMap<String, String> {
    let mut headers = HashMap::new();
    let mut current_name: Option<String> = None;
    let mut current_value = String::new();

    for line in input.lines() {
        if line.starts_with(' ') || line.starts_with('\t') {
            if !current_value.is_empty() {
                current_value.push(' ');
            }
            current_value.push_str(line.trim());
            continue;
        }

        if let Some(name) = current_name.take() {
            headers.insert(name, current_value.trim().to_string());
            current_value.clear();
        }

        if let Some((name, value)) = line.split_once(':') {
            current_name = Some(name.trim().to_lowercase());
            current_value.push_str(value.trim());
        }
    }

    if let Some(name) = current_name {
        headers.insert(name, current_value.trim().to_string());
    }

    headers
}

fn unfolded_headers(raw_message: &[u8]) -> Vec<String> {
    let mut headers = Vec::new();
    let mut current = String::new();

    for line in String::from_utf8_lossy(raw_message).lines() {
        if line.trim().is_empty() {
            break;
        }

        if line.starts_with(' ') || line.starts_with('\t') {
            current.push(' ');
            current.push_str(line.trim());
        } else {
            if !current.is_empty() {
                headers.push(current);
            }
            current = line.trim_end_matches('\r').to_string();
        }
    }

    if !current.is_empty() {
        headers.push(current);
    }

    headers
}

fn parse_address_list(value: &str) -> Vec<ParsedMailAddress> {
    value.split(',').filter_map(parse_single_address).collect()
}

fn parse_single_address(value: &str) -> Option<ParsedMailAddress> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return None;
    }

    if let Some((display, address)) = trimmed.rsplit_once('<') {
        let email = normalize_email(address.trim().trim_end_matches('>'));
        if email.is_empty() {
            return None;
        }
        let display_name = display.trim().trim_matches('"').trim().to_string();
        return Some(ParsedMailAddress {
            email,
            display_name: (!display_name.is_empty()).then_some(display_name),
        });
    }

    let email = normalize_email(trimmed.trim_matches(['<', '>']).trim_matches('"'));
    if email.is_empty() {
        None
    } else {
        Some(ParsedMailAddress {
            email,
            display_name: None,
        })
    }
}

fn trim_trailing_crlf(bytes: &mut Vec<u8>) {
    while matches!(bytes.last(), Some(b'\r' | b'\n')) {
        bytes.pop();
    }
}

#[cfg(test)]
mod tests {
    use super::{parse_header_recipients, parse_message_attachments, parse_rfc822_message};

    #[test]
    fn parse_header_recipients_unfolds_and_normalizes_addresses() {
        let raw = concat!(
            "From: Sender <sender@example.test>\r\n",
            "To: Primary <to@example.test>,\r\n",
            "  Secondary <second@example.test>\r\n",
            "Cc: copy@example.test\r\n",
            "\r\n",
            "Body\r\n"
        );

        let to = parse_header_recipients(raw.as_bytes(), "to");
        let cc = parse_header_recipients(raw.as_bytes(), "cc");

        assert_eq!(to.len(), 2);
        assert_eq!(to[0].address, "to@example.test");
        assert_eq!(to[0].display_name.as_deref(), Some("Primary"));
        assert_eq!(to[1].address, "second@example.test");
        assert_eq!(cc[0].address, "copy@example.test");
    }

    #[test]
    fn parse_message_attachments_keeps_filename_mime_and_bytes() {
        let message = concat!(
            "Content-Type: multipart/mixed; boundary=\"abc\"\r\n",
            "\r\n",
            "--abc\r\n",
            "Content-Type: text/plain\r\n",
            "\r\n",
            "Hello\r\n",
            "--abc\r\n",
            "Content-Type: application/pdf\r\n",
            "Content-Disposition: attachment; filename=\"invoice.pdf\"\r\n",
            "\r\n",
            "PDFDATA\r\n",
            "--abc--\r\n"
        );

        let attachments = parse_message_attachments(message.as_bytes()).unwrap();

        assert_eq!(attachments.len(), 1);
        assert_eq!(attachments[0].file_name, "invoice.pdf");
        assert_eq!(attachments[0].media_type, "application/pdf");
        assert_eq!(attachments[0].blob_bytes, b"PDFDATA\r\n".to_vec());
    }

    #[test]
    fn parse_rfc822_message_collects_headers_body_and_attachments() {
        let message = concat!(
            "From: Alice <alice@example.test>\r\n",
            "To: Bob <bob@example.test>\r\n",
            "Subject: Import\r\n",
            "Message-Id: <id@example.test>\r\n",
            "Content-Type: multipart/mixed; boundary=\"b1\"\r\n",
            "\r\n",
            "--b1\r\n",
            "Content-Type: text/plain\r\n",
            "\r\n",
            "Hello\r\n",
            "--b1\r\n",
            "Content-Type: application/vnd.oasis.opendocument.text\r\n",
            "Content-Disposition: attachment; filename=\"notes.odt\"\r\n",
            "\r\n",
            "ODT-DATA\r\n",
            "--b1--\r\n"
        );

        let parsed = parse_rfc822_message(message.as_bytes()).unwrap();

        assert_eq!(parsed.subject, "Import");
        assert_eq!(parsed.message_id.as_deref(), Some("<id@example.test>"));
        assert_eq!(parsed.from.unwrap().email, "alice@example.test");
        assert_eq!(parsed.to.len(), 1);
        assert_eq!(parsed.attachments.len(), 1);
        assert_eq!(parsed.attachments[0].file_name, "notes.odt");
        assert_eq!(
            parsed.attachments[0].media_type,
            "application/vnd.oasis.opendocument.text"
        );
        assert_eq!(parsed.attachments[0].blob_bytes, b"ODT-DATA".to_vec());
    }
}
