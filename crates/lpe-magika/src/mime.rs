use anyhow::{anyhow, Result};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use std::collections::HashMap;

use crate::types::{MimeAttachmentPart, VisibleBodyParts};

#[derive(Debug)]
struct ParsedVisiblePart {
    content_type: String,
    body_text: String,
}

pub fn collect_mime_attachment_parts(bytes: &[u8]) -> Result<Vec<MimeAttachmentPart>> {
    let mut attachments = Vec::new();
    collect_attachment_parts(bytes, &mut attachments)?;
    Ok(attachments)
}

pub fn parse_rfc822_header_value(bytes: &[u8], name: &str) -> Option<String> {
    let (header_block, _) = split_headers_and_body_bytes(bytes);
    let headers = parse_rfc822_headers_bytes(header_block);
    headers
        .get(&name.to_ascii_lowercase())
        .map(|value| decode_rfc2047_words(value).trim().to_string())
        .filter(|value| !value.is_empty())
}

pub fn extract_visible_text(bytes: &[u8]) -> Result<String> {
    Ok(parse_visible_part(bytes)?.body_text.trim().to_string())
}

pub fn extract_visible_body_parts(bytes: &[u8]) -> Result<VisibleBodyParts> {
    let part = parse_visible_part(bytes)?;
    let html_body = if part
        .content_type
        .to_ascii_lowercase()
        .starts_with("text/html")
    {
        let body = String::from_utf8_lossy(&decode_transfer_encoding(
            split_headers_and_body_bytes(bytes).1,
            &parse_rfc822_headers_bytes(split_headers_and_body_bytes(bytes).0)
                .get("content-transfer-encoding")
                .map(|value| value.to_ascii_lowercase())
                .unwrap_or_default(),
        )?)
        .trim()
        .to_string();
        (!body.is_empty()).then_some(body)
    } else {
        first_html_part(bytes)?
    };
    Ok(VisibleBodyParts {
        text_body: part.body_text.trim().to_string(),
        html_body,
    })
}

fn collect_attachment_parts(bytes: &[u8], attachments: &mut Vec<MimeAttachmentPart>) -> Result<()> {
    let (header_block, body_block) = split_headers_and_body_bytes(bytes);
    let headers = parse_rfc822_headers_bytes(header_block);
    let content_type = headers
        .get("content-type")
        .cloned()
        .unwrap_or_else(|| "text/plain".to_string());
    let transfer_encoding = headers
        .get("content-transfer-encoding")
        .map(|value| value.to_ascii_lowercase())
        .unwrap_or_default();
    let decoded_body = decode_transfer_encoding(body_block, &transfer_encoding)?;

    if content_type.to_ascii_lowercase().starts_with("multipart/") {
        let Some(boundary) = content_type_parameter(&content_type, "boundary") else {
            return Ok(());
        };
        for part in split_multipart_parts(&decoded_body, &boundary) {
            collect_attachment_parts(&part, attachments)?;
        }
        return Ok(());
    }

    let content_disposition = headers.get("content-disposition").cloned();
    let filename = content_disposition
        .as_deref()
        .and_then(|value| content_type_parameter(value, "filename"))
        .or_else(|| content_type_parameter(&content_type, "name"));
    let is_attachment = content_disposition
        .as_deref()
        .map(|value| value.to_ascii_lowercase().starts_with("attachment"))
        .unwrap_or(false);
    if is_attachment || filename.is_some() {
        attachments.push(MimeAttachmentPart {
            filename,
            declared_mime: Some(strip_content_type_parameters(&content_type)),
            content_disposition,
            bytes: decoded_body,
        });
    }
    Ok(())
}

fn parse_visible_part(bytes: &[u8]) -> Result<ParsedVisiblePart> {
    let (header_block, body_block) = split_headers_and_body_bytes(bytes);
    let headers = parse_rfc822_headers_bytes(header_block);
    let content_type = headers
        .get("content-type")
        .cloned()
        .unwrap_or_else(|| "text/plain".to_string());
    let transfer_encoding = headers
        .get("content-transfer-encoding")
        .map(|value| value.to_ascii_lowercase())
        .unwrap_or_default();
    let decoded_body = decode_transfer_encoding(body_block, &transfer_encoding)?;
    let content_type_lower = content_type.to_ascii_lowercase();

    let body_text = if content_type_lower.starts_with("multipart/") {
        match content_type_parameter(&content_type, "boundary") {
            Some(boundary) => {
                let mut text_plain = None;
                let mut text_html = None;

                for part in split_multipart_parts(&decoded_body, &boundary) {
                    let nested = parse_visible_part(&part)?;
                    let nested_type = nested.content_type.to_ascii_lowercase();
                    if nested_type.starts_with("text/plain")
                        && !nested.body_text.trim().is_empty()
                        && text_plain.is_none()
                    {
                        text_plain = Some(nested.body_text);
                    } else if nested_type.starts_with("text/html")
                        && !nested.body_text.trim().is_empty()
                        && text_html.is_none()
                    {
                        text_html = Some(nested.body_text);
                    } else if nested_type.starts_with("multipart/")
                        && !nested.body_text.trim().is_empty()
                        && text_plain.is_none()
                        && text_html.is_none()
                    {
                        text_plain = Some(nested.body_text);
                    }
                }

                text_plain.or(text_html).unwrap_or_default()
            }
            None => String::from_utf8_lossy(&decoded_body).to_string(),
        }
    } else if content_type_lower.starts_with("text/html") {
        html_to_text(&String::from_utf8_lossy(&decoded_body))
    } else if content_type_lower.starts_with("text/plain")
        || content_type_lower.starts_with("message/rfc822")
    {
        String::from_utf8_lossy(&decoded_body).to_string()
    } else {
        String::new()
    };

    Ok(ParsedVisiblePart {
        content_type,
        body_text,
    })
}

fn first_html_part(bytes: &[u8]) -> Result<Option<String>> {
    let (header_block, body_block) = split_headers_and_body_bytes(bytes);
    let headers = parse_rfc822_headers_bytes(header_block);
    let content_type = headers
        .get("content-type")
        .cloned()
        .unwrap_or_else(|| "text/plain".to_string());
    let transfer_encoding = headers
        .get("content-transfer-encoding")
        .map(|value| value.to_ascii_lowercase())
        .unwrap_or_default();
    let decoded_body = decode_transfer_encoding(body_block, &transfer_encoding)?;
    let content_type_lower = content_type.to_ascii_lowercase();

    if content_type_lower.starts_with("multipart/") {
        if let Some(boundary) = content_type_parameter(&content_type, "boundary") {
            for part in split_multipart_parts(&decoded_body, &boundary) {
                if let Some(html) = first_html_part(&part)? {
                    if !html.trim().is_empty() {
                        return Ok(Some(html));
                    }
                }
            }
        }
        return Ok(None);
    }

    if content_type_lower.starts_with("text/html") {
        let html = String::from_utf8_lossy(&decoded_body).trim().to_string();
        return Ok((!html.is_empty()).then_some(html));
    }

    Ok(None)
}

fn strip_content_type_parameters(value: &str) -> String {
    value
        .split(';')
        .next()
        .unwrap_or_default()
        .trim()
        .to_string()
}

fn split_headers_and_body_bytes(raw: &[u8]) -> (&[u8], &[u8]) {
    for delimiter in [b"\r\n\r\n".as_slice(), b"\n\n".as_slice()] {
        if let Some(index) = raw
            .windows(delimiter.len())
            .position(|window| window == delimiter)
        {
            return (&raw[..index], &raw[index + delimiter.len()..]);
        }
    }
    (raw, &[])
}

fn parse_rfc822_headers_bytes(block: &[u8]) -> HashMap<String, String> {
    let raw = String::from_utf8_lossy(block);
    parse_rfc822_headers(raw.as_ref())
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
        let Some((name, value)) = line.trim_end_matches('\r').split_once(':') else {
            continue;
        };
        current_name = name.trim().to_ascii_lowercase();
        headers.insert(current_name.clone(), value.trim().to_string());
    }
    headers
}

fn split_multipart_parts(body: &[u8], boundary: &str) -> Vec<Vec<u8>> {
    let boundary_marker = format!("--{boundary}").into_bytes();
    let closing_marker = format!("--{boundary}--").into_bytes();
    let mut parts = Vec::new();
    let mut current = Vec::new();
    let mut in_part = false;

    for line in split_lines_inclusive(body) {
        let trimmed = trim_ascii_line_end(line);
        if trimmed == boundary_marker.as_slice() {
            if in_part && !current.is_empty() {
                parts.push(std::mem::take(&mut current));
            }
            in_part = true;
            current.clear();
            continue;
        }
        if trimmed == closing_marker.as_slice() {
            if in_part && !current.is_empty() {
                parts.push(std::mem::take(&mut current));
            }
            break;
        }
        if in_part {
            current.extend_from_slice(line);
        }
    }

    parts
}

fn split_lines_inclusive(bytes: &[u8]) -> Vec<&[u8]> {
    let mut lines = Vec::new();
    let mut start = 0usize;
    for (index, byte) in bytes.iter().enumerate() {
        if *byte == b'\n' {
            lines.push(&bytes[start..=index]);
            start = index + 1;
        }
    }
    if start < bytes.len() {
        lines.push(&bytes[start..]);
    }
    lines
}

fn trim_ascii_line_end(bytes: &[u8]) -> &[u8] {
    let mut end = bytes.len();
    while end > 0 && matches!(bytes[end - 1], b'\r' | b'\n') {
        end -= 1;
    }
    &bytes[..end]
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
