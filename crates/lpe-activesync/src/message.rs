use anyhow::{anyhow, bail, Result};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use lpe_storage::{JmapEmail, JmapEmailAddress, SubmitMessageInput, SubmittedRecipientInput};
use std::collections::HashMap;
use uuid::Uuid;

use crate::{types::AuthenticatedPrincipal, wbxml::WbxmlNode};

#[derive(Debug, Clone)]
pub(crate) struct ParsedMailbox {
    pub(crate) address: String,
    pub(crate) display_name: Option<String>,
}

#[derive(Debug)]
pub(crate) struct ParsedMimeMessage {
    pub(crate) from: Option<ParsedMailbox>,
    pub(crate) to: Vec<SubmittedRecipientInput>,
    pub(crate) cc: Vec<SubmittedRecipientInput>,
    pub(crate) bcc: Vec<SubmittedRecipientInput>,
    pub(crate) subject: String,
    pub(crate) body_text: String,
    pub(crate) internet_message_id: Option<String>,
}

pub(crate) fn parse_mime_message(bytes: &[u8]) -> Result<ParsedMimeMessage> {
    let message = parse_message_part(bytes)?;
    Ok(ParsedMimeMessage {
        from: message
            .headers
            .get("from")
            .and_then(|value| parse_mailbox(value).ok().flatten()),
        to: parse_address_list(message.headers.get("to").map(String::as_str).unwrap_or("")),
        cc: parse_address_list(message.headers.get("cc").map(String::as_str).unwrap_or("")),
        bcc: parse_address_list(message.headers.get("bcc").map(String::as_str).unwrap_or("")),
        subject: message
            .headers
            .get("subject")
            .map(|value| decode_rfc2047_words(value).trim().to_string())
            .unwrap_or_default(),
        body_text: message.body_text.trim().to_string(),
        internet_message_id: message
            .headers
            .get("message-id")
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty()),
    })
}

#[derive(Debug)]
struct ParsedMessagePart {
    headers: HashMap<String, String>,
    body_text: String,
}

fn parse_message_part(bytes: &[u8]) -> Result<ParsedMessagePart> {
    let raw = String::from_utf8_lossy(bytes);
    let (header_block, body_block) = split_headers_and_body(raw.as_ref());
    let headers = parse_rfc822_headers(header_block);
    let content_type = headers
        .get("content-type")
        .cloned()
        .unwrap_or_else(|| "text/plain".to_string());
    let transfer_encoding = headers
        .get("content-transfer-encoding")
        .map(|value| value.to_ascii_lowercase())
        .unwrap_or_default();

    let decoded_body = decode_transfer_encoding(body_block.as_bytes(), &transfer_encoding)?;
    let body_text = if content_type.to_ascii_lowercase().starts_with("multipart/") {
        parse_multipart_body(&content_type, &decoded_body)?
    } else if content_type.to_ascii_lowercase().starts_with("text/html") {
        html_to_text(&String::from_utf8_lossy(&decoded_body))
    } else {
        String::from_utf8_lossy(&decoded_body).to_string()
    };

    Ok(ParsedMessagePart { headers, body_text })
}

fn split_headers_and_body(raw: &str) -> (&str, &str) {
    raw.split_once("\r\n\r\n")
        .or_else(|| raw.split_once("\n\n"))
        .unwrap_or((raw, ""))
}

fn parse_multipart_body(content_type: &str, body: &[u8]) -> Result<String> {
    let Some(boundary) = content_type_parameter(content_type, "boundary") else {
        return Ok(String::from_utf8_lossy(body).to_string());
    };
    let boundary_marker = format!("--{boundary}");
    let raw = String::from_utf8_lossy(body);
    let mut text_plain = None;
    let mut text_html = None;

    for part in raw.split(&boundary_marker).skip(1) {
        let trimmed = part.trim();
        if trimmed.is_empty() || trimmed == "--" {
            continue;
        }
        let trimmed = trimmed.trim_start_matches("\r\n").trim_start_matches('\n');
        let trimmed = trimmed.trim_end_matches("--").trim();
        let nested = parse_message_part(trimmed.as_bytes())?;
        let part_type = nested
            .headers
            .get("content-type")
            .map(|value| value.to_ascii_lowercase())
            .unwrap_or_else(|| "text/plain".to_string());
        if part_type.starts_with("text/plain") && text_plain.is_none() {
            text_plain = Some(nested.body_text);
        } else if part_type.starts_with("text/html") && text_html.is_none() {
            text_html = Some(nested.body_text);
        } else if part_type.starts_with("multipart/") && text_plain.is_none() && text_html.is_none()
        {
            text_plain = Some(nested.body_text);
        }
    }

    Ok(text_plain.or(text_html).unwrap_or_default())
}

fn content_type_parameter(header_value: &str, parameter: &str) -> Option<String> {
    for segment in header_value.split(';').skip(1) {
        let (name, value) = segment.split_once('=')?;
        if name.trim().eq_ignore_ascii_case(parameter) {
            return Some(value.trim().trim_matches('"').to_string());
        }
    }
    None
}

fn decode_transfer_encoding(body: &[u8], encoding: &str) -> Result<Vec<u8>> {
    match encoding.trim() {
        "base64" => {
            let compact = String::from_utf8_lossy(body)
                .lines()
                .map(str::trim)
                .collect::<String>();
            Ok(BASE64.decode(compact)?)
        }
        "quoted-printable" => decode_quoted_printable(body),
        _ => Ok(body.to_vec()),
    }
}

fn decode_quoted_printable(body: &[u8]) -> Result<Vec<u8>> {
    let mut output = Vec::with_capacity(body.len());
    let mut cursor = 0usize;
    while cursor < body.len() {
        match body[cursor] {
            b'=' => {
                if cursor + 1 < body.len()
                    && (body[cursor + 1] == b'\n' || body[cursor + 1] == b'\r')
                {
                    cursor += 1;
                    while cursor < body.len() && (body[cursor] == b'\n' || body[cursor] == b'\r') {
                        cursor += 1;
                    }
                    continue;
                }
                let hex = body
                    .get(cursor + 1..cursor + 3)
                    .ok_or_else(|| anyhow!("invalid quoted-printable sequence"))?;
                let value = std::str::from_utf8(hex)?;
                output.push(u8::from_str_radix(value, 16)?);
                cursor += 3;
            }
            byte => {
                output.push(byte);
                cursor += 1;
            }
        }
    }
    Ok(output)
}

fn decode_rfc2047_words(value: &str) -> String {
    let mut decoded = String::new();
    let mut rest = value;
    while let Some(start) = rest.find("=?") {
        decoded.push_str(&rest[..start]);
        let candidate = &rest[start + 2..];
        let Some(charset_end) = candidate.find('?') else {
            decoded.push_str(&rest[start..]);
            return decoded;
        };
        let charset = &candidate[..charset_end];
        let candidate = &candidate[charset_end + 1..];
        let Some(encoding_end) = candidate.find('?') else {
            decoded.push_str(&rest[start..]);
            return decoded;
        };
        let encoding = &candidate[..encoding_end];
        let candidate = &candidate[encoding_end + 1..];
        let Some(payload_end) = candidate.find("?=") else {
            decoded.push_str(&rest[start..]);
            return decoded;
        };
        let payload = &candidate[..payload_end];
        let segment = decode_rfc2047_word(charset, encoding, payload).unwrap_or_else(|| {
            rest[start..start + 2 + charset_end + 1 + encoding_end + 1 + payload_end + 2]
                .to_string()
        });
        decoded.push_str(&segment);
        rest = &candidate[payload_end + 2..];
    }
    decoded.push_str(rest);
    decoded
}

fn decode_rfc2047_word(charset: &str, encoding: &str, payload: &str) -> Option<String> {
    if !charset.eq_ignore_ascii_case("utf-8") && !charset.eq_ignore_ascii_case("us-ascii") {
        return None;
    }
    match encoding {
        "B" | "b" => BASE64
            .decode(payload)
            .ok()
            .and_then(|bytes| String::from_utf8(bytes).ok()),
        "Q" | "q" => {
            let qp = payload.replace('_', " ");
            decode_quoted_printable(qp.as_bytes())
                .ok()
                .and_then(|bytes| String::from_utf8(bytes).ok())
        }
        _ => None,
    }
}

fn parse_rfc822_headers(block: &str) -> HashMap<String, String> {
    let mut headers: HashMap<String, String> = HashMap::new();
    let mut current_name = String::new();
    for line in block.lines() {
        if line.starts_with(' ') || line.starts_with('\t') {
            if let Some(value) = headers.get_mut(&current_name) {
                value.push(' ');
                value.push_str(line.trim());
            }
            continue;
        }

        if let Some((name, value)) = line.split_once(':') {
            current_name = name.trim().to_lowercase();
            headers.insert(current_name.clone(), value.trim().to_string());
        }
    }
    headers
}

pub(crate) fn parse_address_list(value: &str) -> Vec<SubmittedRecipientInput> {
    split_addresses(value)
        .into_iter()
        .filter_map(|entry| parse_mailbox(&entry).ok().flatten())
        .map(|mailbox| SubmittedRecipientInput {
            address: mailbox.address,
            display_name: mailbox.display_name,
        })
        .collect()
}

pub(crate) fn parse_mailbox(value: &str) -> Result<Option<ParsedMailbox>> {
    let decoded = decode_rfc2047_words(value);
    let trimmed = decoded.trim().trim_end_matches(';').trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    if let (Some(start), Some(end)) = (trimmed.rfind('<'), trimmed.rfind('>')) {
        let address = trimmed[start + 1..end].trim();
        if address.is_empty() {
            bail!("mailbox address is empty");
        }
        let display_name = trimmed[..start].trim().trim_matches('"').trim().to_string();
        return Ok(Some(ParsedMailbox {
            address: address.to_string(),
            display_name: (!display_name.is_empty()).then_some(display_name),
        }));
    }
    Ok(Some(ParsedMailbox {
        address: trimmed.to_string(),
        display_name: None,
    }))
}

fn split_addresses(value: &str) -> Vec<String> {
    let mut addresses = Vec::new();
    let mut current = String::new();
    let mut in_quotes = false;
    let mut angle_depth = 0u8;
    for ch in value.chars() {
        match ch {
            '"' => {
                in_quotes = !in_quotes;
                current.push(ch);
            }
            '<' if !in_quotes => {
                angle_depth = angle_depth.saturating_add(1);
                current.push(ch);
            }
            '>' if !in_quotes && angle_depth > 0 => {
                angle_depth -= 1;
                current.push(ch);
            }
            ',' if !in_quotes && angle_depth == 0 => {
                let trimmed = current.trim();
                if !trimmed.is_empty() {
                    addresses.push(trimmed.to_string());
                }
                current.clear();
            }
            _ => current.push(ch),
        }
    }
    let trimmed = current.trim();
    if !trimmed.is_empty() {
        addresses.push(trimmed.to_string());
    }
    addresses
}

pub(crate) fn format_email_address(address: &JmapEmailAddress) -> String {
    address
        .display_name
        .as_deref()
        .filter(|name| !name.is_empty())
        .map(|name| format!("{name} <{}>", address.address))
        .unwrap_or_else(|| address.address.clone())
}

pub(crate) fn merged_draft_input(
    principal: &AuthenticatedPrincipal,
    draft_id: Uuid,
    existing: &JmapEmail,
    application_data: &WbxmlNode,
) -> SubmitMessageInput {
    let from_mailbox =
        field_text(application_data, "From").and_then(|value| parse_mailbox(&value).ok().flatten());
    SubmitMessageInput {
        draft_message_id: Some(draft_id),
        account_id: principal.account_id,
        source: "activesync-sync-change".to_string(),
        from_display: from_mailbox
            .as_ref()
            .and_then(|mailbox| mailbox.display_name.clone())
            .or_else(|| Some(principal.display_name.clone())),
        from_address: from_mailbox
            .map(|mailbox| mailbox.address)
            .unwrap_or_else(|| existing.from_address.clone()),
        to: field_text(application_data, "To")
            .map(|value| parse_address_list(&value))
            .unwrap_or_else(|| {
                existing
                    .to
                    .iter()
                    .map(|recipient| SubmittedRecipientInput {
                        address: recipient.address.clone(),
                        display_name: recipient.display_name.clone(),
                    })
                    .collect()
            }),
        cc: field_text(application_data, "Cc")
            .map(|value| parse_address_list(&value))
            .unwrap_or_else(|| {
                existing
                    .cc
                    .iter()
                    .map(|recipient| SubmittedRecipientInput {
                        address: recipient.address.clone(),
                        display_name: recipient.display_name.clone(),
                    })
                    .collect()
            }),
        bcc: existing
            .bcc
            .iter()
            .map(|recipient| SubmittedRecipientInput {
                address: recipient.address.clone(),
                display_name: recipient.display_name.clone(),
            })
            .collect(),
        subject: field_text(application_data, "Subject")
            .unwrap_or_else(|| existing.subject.clone()),
        body_text: application_data
            .child("Body")
            .and_then(|body| body.child("Data"))
            .map(|node| node.text_value().to_string())
            .unwrap_or_else(|| existing.body_text.clone()),
        body_html_sanitized: existing.body_html_sanitized.clone(),
        internet_message_id: existing.internet_message_id.clone(),
        mime_blob_ref: Some(format!("draft-message:{draft_id}")),
        size_octets: existing.size_octets,
    }
}

pub(crate) fn draft_input_from_application_data(
    principal: &AuthenticatedPrincipal,
    draft_message_id: Option<Uuid>,
    application_data: &WbxmlNode,
    source: &str,
) -> SubmitMessageInput {
    let from_mailbox =
        field_text(application_data, "From").and_then(|value| parse_mailbox(&value).ok().flatten());
    SubmitMessageInput {
        draft_message_id,
        account_id: principal.account_id,
        source: source.to_string(),
        from_display: from_mailbox
            .as_ref()
            .and_then(|mailbox| mailbox.display_name.clone())
            .or_else(|| Some(principal.display_name.clone())),
        from_address: from_mailbox
            .map(|mailbox| mailbox.address)
            .unwrap_or_else(|| principal.email.clone()),
        to: field_text(application_data, "To")
            .map(|value| parse_address_list(&value))
            .unwrap_or_default(),
        cc: field_text(application_data, "Cc")
            .map(|value| parse_address_list(&value))
            .unwrap_or_default(),
        bcc: Vec::new(),
        subject: field_text(application_data, "Subject").unwrap_or_default(),
        body_text: application_data
            .child("Body")
            .and_then(|body| body.child("Data"))
            .map(|node| node.text_value().to_string())
            .unwrap_or_default(),
        body_html_sanitized: None,
        internet_message_id: None,
        mime_blob_ref: None,
        size_octets: 0,
    }
}

pub(crate) fn field_text(node: &WbxmlNode, name: &str) -> Option<String> {
    node.child(name)
        .map(|child| child.text_value().trim().to_string())
        .filter(|value| !value.is_empty())
}

fn html_to_text(value: &str) -> String {
    let mut output = String::with_capacity(value.len());
    let mut in_tag = false;
    for ch in value.chars() {
        match ch {
            '<' => in_tag = true,
            '>' => {
                in_tag = false;
                output.push(' ');
            }
            _ if !in_tag => output.push(ch),
            _ => {}
        }
    }
    output.split_whitespace().collect::<Vec<_>>().join(" ")
}

pub(crate) fn split_name(value: &str) -> (String, String) {
    let trimmed = value.trim();
    if let Some((first, last)) = trimmed.split_once(' ') {
        (first.trim().to_string(), last.trim().to_string())
    } else {
        (trimmed.to_string(), String::new())
    }
}

pub(crate) fn activesync_timestamp(value: &str) -> String {
    value.replace('-', "").replace(':', "").replace('"', "")
}
