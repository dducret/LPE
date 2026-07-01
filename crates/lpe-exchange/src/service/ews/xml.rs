use anyhow::{anyhow, bail, Result};
use axum::{
    http::{header::CONTENT_TYPE, HeaderMap, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
};

pub(in crate::service) fn xml_response(status: StatusCode, body: String) -> Response {
    let mut response = (status, body).into_response();
    response.headers_mut().insert(
        CONTENT_TYPE,
        HeaderValue::from_static("text/xml; charset=utf-8"),
    );
    response
}

pub(in crate::service) fn soap_response(body: String) -> Response {
    let envelope = format!(
        concat!(
            "<?xml version=\"1.0\" encoding=\"utf-8\"?>",
            "<s:Envelope xmlns:s=\"http://schemas.xmlsoap.org/soap/envelope/\" ",
            "xmlns:m=\"http://schemas.microsoft.com/exchange/services/2006/messages\" ",
            "xmlns:t=\"http://schemas.microsoft.com/exchange/services/2006/types\">",
            "<s:Header><t:ServerVersionInfo MajorVersion=\"15\" MinorVersion=\"0\" MajorBuildNumber=\"0\" MinorBuildNumber=\"0\" Version=\"Exchange2013\"/></s:Header>",
            "<s:Body>{body}</s:Body>",
            "</s:Envelope>"
        ),
        body = body,
    );
    xml_response(StatusCode::OK, envelope)
}

pub(in crate::service) fn decode_ews_body(headers: &HeaderMap, body: &[u8]) -> Result<String> {
    let content_type = headers
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_ascii_lowercase()
        .chars()
        .filter(|character| {
            !character.is_ascii_whitespace() && *character != '"' && *character != '\''
        })
        .collect::<String>();

    if body.starts_with(&[0xff, 0xfe]) {
        return decode_utf16_body(&body[2..], true);
    }
    if body.starts_with(&[0xfe, 0xff]) {
        return decode_utf16_body(&body[2..], false);
    }
    if content_type.contains("charset=utf-16be") {
        return decode_utf16_body(body, false);
    }
    if content_type.contains("charset=utf-16le") || content_type.contains("charset=utf-16") {
        return decode_utf16_body(body, true);
    }

    std::str::from_utf8(body)
        .map(str::to_string)
        .map_err(|_| anyhow!("EWS request body is not valid UTF-8 or UTF-16"))
}

fn decode_utf16_body(body: &[u8], little_endian: bool) -> Result<String> {
    let mut chunks = body.chunks_exact(2);
    let words = chunks
        .by_ref()
        .map(|chunk| {
            if little_endian {
                u16::from_le_bytes([chunk[0], chunk[1]])
            } else {
                u16::from_be_bytes([chunk[0], chunk[1]])
            }
        })
        .collect::<Vec<_>>();
    if !chunks.remainder().is_empty() {
        return Err(anyhow!("EWS UTF-16 request body has an odd byte length"));
    }
    String::from_utf16(&words).map_err(|_| anyhow!("EWS request body is not valid UTF-16"))
}

pub(in crate::service) fn operation_name(body: &str) -> Option<String> {
    let body_start = body.find(":Body").or_else(|| body.find("<Body"))?;
    let body_content_start = body[body_start..].find('>')? + body_start + 1;
    let mut remaining = &body[body_content_start..];

    loop {
        let tag_start = remaining.find('<')?;
        let tag_text = remaining[tag_start + 1..].trim_start();
        if tag_text.starts_with('/') {
            return None;
        }
        if tag_text.starts_with('?') || tag_text.starts_with('!') {
            remaining = &tag_text[1..];
            continue;
        }

        let tag_end = tag_text
            .find(|value: char| value.is_whitespace() || value == '/' || value == '>')
            .unwrap_or(tag_text.len());
        let qualified_name = &tag_text[..tag_end];
        let local_name = qualified_name.rsplit(':').next()?;
        if local_name
            .chars()
            .all(|value| value.is_ascii_alphanumeric() || value == '_')
        {
            return Some(match local_name {
                "GetUserAvailabilityRequest" => "GetUserAvailability".to_string(),
                value if value.ends_with("Request") => {
                    value.trim_end_matches("Request").to_string()
                }
                _ => local_name.to_string(),
            });
        }
        return None;
    }
}

pub(in crate::service) fn escape_xml(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

pub(in crate::service) fn attribute_values_for_tag<'a>(
    xml: &'a str,
    local_name: &str,
    attr: &str,
) -> Vec<&'a str> {
    let mut values = Vec::new();
    let mut rest = xml;
    while let Some(tag_start) = rest.find('<') {
        let tag_text = rest[tag_start + 1..].trim_start();
        if tag_text.starts_with('/') || tag_text.starts_with('?') || tag_text.starts_with('!') {
            rest = &tag_text[1..];
            continue;
        }
        let Some(tag_end) = tag_text.find('>') else {
            break;
        };
        let open_tag = &tag_text[..tag_end];
        let Some(qualified_name) = open_tag
            .split(|value: char| value.is_whitespace() || value == '/')
            .next()
        else {
            rest = &tag_text[tag_end + 1..];
            continue;
        };
        if qualified_name.rsplit(':').next() == Some(local_name) {
            if let Some(value) = attribute_value(open_tag, attr) {
                values.push(value);
            }
        }
        rest = &tag_text[tag_end + 1..];
    }
    values
}

pub(in crate::service) fn attribute_value_after<'a>(
    body: &'a str,
    tag: &str,
    attr: &str,
) -> Option<&'a str> {
    let index = body.find(tag)?;
    let rest = &body[index..];
    let end = rest.find('>')?;
    let tag_text = &rest[..end];
    attribute_value(tag_text, attr)
}

pub(in crate::service) fn ews_bool_attribute(body: &str, tag: &str, attr: &str) -> Option<bool> {
    attribute_value_after(body, tag, attr)
        .map(|value| value.eq_ignore_ascii_case("true") || value == "1")
}

pub(in crate::service) fn ews_usize_attribute(body: &str, tag: &str, attr: &str) -> Option<usize> {
    attribute_value_after(body, tag, attr)?.parse().ok()
}

pub(in crate::service) fn count_folder_elements(value: &str) -> usize {
    count_tag_occurrences(value, "<t:Folder>")
        + count_tag_occurrences(value, "<t:ContactsFolder>")
        + count_tag_occurrences(value, "<t:CalendarFolder>")
        + count_tag_occurrences(value, "<t:TasksFolder>")
}

pub(in crate::service) fn count_tag_occurrences(value: &str, needle: &str) -> usize {
    value.match_indices(needle).count()
}

pub(in crate::service) fn attribute_value<'a>(tag_text: &'a str, attr: &str) -> Option<&'a str> {
    let pattern = format!("{attr}=");
    let start = tag_text.find(&pattern)? + pattern.len();
    let quote = tag_text[start..].chars().next()?;
    if quote != '"' && quote != '\'' {
        return None;
    }
    let value_start = start + quote.len_utf8();
    let value_end = tag_text[value_start..].find(quote)? + value_start;
    Some(&tag_text[value_start..value_end])
}

pub(in crate::service) fn open_tag_text<'a>(xml: &'a str, local_name: &str) -> Option<&'a str> {
    let mut rest = xml;
    while let Some(tag_start) = rest.find('<') {
        let tag_text = rest[tag_start + 1..].trim_start();
        if tag_text.starts_with('/') || tag_text.starts_with('?') || tag_text.starts_with('!') {
            rest = &tag_text[1..];
            continue;
        }
        let tag_end = tag_text.find('>')?;
        let open_tag = &tag_text[..tag_end];
        let qualified_name = open_tag
            .split(|value: char| value.is_whitespace() || value == '/')
            .next()?;
        if qualified_name.rsplit(':').next()? == local_name {
            return Some(open_tag);
        }
        rest = &tag_text[tag_end + 1..];
    }
    None
}

pub(in crate::service) fn element_text(xml: &str, local_name: &str) -> Option<String> {
    element_content(xml, local_name).map(xml_text)
}

pub(in crate::service) fn element_content<'a>(xml: &'a str, local_name: &str) -> Option<&'a str> {
    element_contents(xml, local_name).into_iter().next()
}

pub(in crate::service) fn element_contents<'a>(xml: &'a str, local_name: &str) -> Vec<&'a str> {
    let mut values = Vec::new();
    let mut rest = xml;
    while let Some(tag_start) = rest.find('<') {
        let tag_text = rest[tag_start + 1..].trim_start();
        if tag_text.starts_with('/') || tag_text.starts_with('?') || tag_text.starts_with('!') {
            rest = &tag_text[1..];
            continue;
        }
        let Some(tag_end) = tag_text.find('>') else {
            break;
        };
        let open_tag = &tag_text[..tag_end];
        let Some(qualified_name) = open_tag
            .split(|value: char| value.is_whitespace() || value == '/')
            .next()
        else {
            break;
        };
        if qualified_name.rsplit(':').next() != Some(local_name) {
            rest = &tag_text[tag_end + 1..];
            continue;
        }
        if open_tag.trim_end().ends_with('/') {
            values.push("");
            rest = &tag_text[tag_end + 1..];
            continue;
        }

        let content_start = tag_start + 1 + tag_text[..tag_end + 1].len();
        let closing_tag = format!("</{qualified_name}>");
        let Some(relative_end) = rest[content_start..].find(&closing_tag) else {
            break;
        };
        let content_end = content_start + relative_end;
        values.push(&rest[content_start..content_end]);
        rest = &rest[content_end + closing_tag.len()..];
    }
    values
}

pub(in crate::service) fn xml_text(value: &str) -> String {
    value
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&amp;", "&")
        .replace("&quot;", "\"")
        .replace("&apos;", "'")
        .trim()
        .to_string()
}

pub(in crate::service) fn html_to_text(value: &str) -> String {
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

pub(in crate::service) fn parse_xml_bool(value: &str) -> Result<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "true" | "1" => Ok(true),
        "false" | "0" => Ok(false),
        other => bail!("unsupported boolean value {other}"),
    }
}

pub(in crate::service) fn parse_xml_bool_attr(value: &str) -> bool {
    matches!(value.trim().to_ascii_lowercase().as_str(), "true" | "1")
}
