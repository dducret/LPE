use anyhow::{anyhow, bail, Result};
use lpe_domain::{MailboxNamePolicy, MailboxPath};
use std::collections::HashSet;
use uuid::Uuid;

use lpe_storage::{ImapEmail, ImapMailboxState, ImapMimePart, JmapEmailAddress};

use crate::{parse::tokenize, MessageRefKind, SelectedMailbox};

pub(crate) struct FetchAttributes {
    pub(crate) items: Vec<FetchItem>,
    pub(crate) mark_seen: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum FetchItem {
    Uid,
    Flags,
    Modseq,
    InternalDate,
    Rfc822Size,
    Envelope,
    Body,
    BodyStructure,
    BodySection(BodySectionFetch),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BodySectionFetch {
    pub(crate) peek: bool,
    pub(crate) section: String,
    pub(crate) partial: Option<PartialRange>,
    pub(crate) response_label: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PartialRange {
    pub(crate) start: usize,
    pub(crate) length: usize,
}

pub(crate) fn mailbox_name_matches(display_name: &str, role: &str, requested: &str) -> bool {
    if let Some(system_role) = MailboxNamePolicy::system_role_for_display_name(requested) {
        return system_role == role;
    }

    let requested = MailboxPath::parse(requested);
    let Ok(requested) = requested else {
        return false;
    };
    let requested_key = MailboxNamePolicy::canonical_key(requested.as_str());
    let display_key = MailboxNamePolicy::canonical_key(display_name);
    requested_key.collides_with(&display_key)
}

pub(crate) fn render_list_flags(role: &str, legacy_xlist: bool) -> String {
    let mut flags = vec!["\\HasNoChildren"];
    match role {
        "inbox" if legacy_xlist => flags.push("\\Inbox"),
        "sent" => flags.push("\\Sent"),
        "drafts" => flags.push("\\Drafts"),
        "trash" => flags.push("\\Trash"),
        "junk" => flags.push("\\Junk"),
        "archive" => flags.push("\\Archive"),
        _ => {}
    }
    format!("({})", flags.join(" "))
}

pub(crate) fn render_mailbox_name(mailbox: &lpe_storage::JmapMailbox) -> String {
    if mailbox.role == "inbox" {
        "INBOX".to_string()
    } else {
        mailbox.name.clone()
    }
}

pub(crate) fn render_imap_mailbox_response_path(value: &str, utf8_enabled: bool) -> String {
    render_imap_mailbox_string(value, utf8_enabled)
}

pub(crate) fn render_imap_mailbox_string(value: &str, utf8_enabled: bool) -> String {
    if utf8_enabled || value.is_ascii() {
        format!("\"{}\"", sanitize_imap_quoted(value))
    } else {
        format!("{{{}}}\r\n{}", value.len(), value)
    }
}

pub(crate) fn parse_fetch_attributes(input: &str) -> Result<FetchAttributes> {
    let upper = input.trim().to_ascii_uppercase();
    let expanded = match upper.as_str() {
        "ALL" => vec![
            FetchItem::Flags,
            FetchItem::InternalDate,
            FetchItem::Rfc822Size,
            FetchItem::Envelope,
        ],
        "FAST" => vec![
            FetchItem::Flags,
            FetchItem::InternalDate,
            FetchItem::Rfc822Size,
        ],
        "FULL" => vec![
            FetchItem::Flags,
            FetchItem::InternalDate,
            FetchItem::Rfc822Size,
            FetchItem::Envelope,
            FetchItem::Body,
        ],
        _ => parse_fetch_item_list(input)?,
    };
    if expanded.is_empty() {
        bail!("FETCH expects at least one attribute");
    }
    let mark_seen = expanded.iter().any(fetch_item_marks_seen);
    Ok(FetchAttributes {
        items: expanded,
        mark_seen,
    })
}

pub(crate) fn ensure_uid_fetch_attributes(requested: &mut FetchAttributes) {
    if !requested
        .items
        .iter()
        .any(|item| matches!(item, FetchItem::Uid))
    {
        requested.items.insert(0, FetchItem::Uid);
    }
}

pub(crate) fn render_fetch_response(
    sequence: usize,
    email: &ImapEmail,
    requested: &FetchAttributes,
) -> Result<Vec<u8>> {
    let mut output = Vec::new();
    output.extend_from_slice(format!("* {} FETCH (", sequence).as_bytes());
    let mut first = true;
    for item in &requested.items {
        if !first {
            output.extend_from_slice(b" ");
        }
        first = false;
        match item {
            FetchItem::Uid => output.extend_from_slice(format!("UID {}", email.uid).as_bytes()),
            FetchItem::Flags => output.extend_from_slice(
                format!("FLAGS ({})", render_flags(email, &email.mailbox_name)).as_bytes(),
            ),
            FetchItem::Modseq => {
                output.extend_from_slice(format!("MODSEQ ({})", email.modseq).as_bytes())
            }
            FetchItem::InternalDate => output.extend_from_slice(
                format!("INTERNALDATE \"{}\"", format_internal_date(email)).as_bytes(),
            ),
            FetchItem::Rfc822Size => output.extend_from_slice(
                format!("RFC822.SIZE {}", message_rfc822_size(email)).as_bytes(),
            ),
            FetchItem::Envelope => {
                output.extend_from_slice(format!("ENVELOPE {}", render_envelope(email)).as_bytes())
            }
            FetchItem::Body => {
                output.extend_from_slice(format!("BODY {}", render_bodystructure(email)).as_bytes())
            }
            FetchItem::BodyStructure => output.extend_from_slice(
                format!("BODYSTRUCTURE {}", render_bodystructure(email)).as_bytes(),
            ),
            FetchItem::BodySection(section) => append_body_section(&mut output, email, section),
        }
    }
    output.extend_from_slice(b")\r\n");
    Ok(output)
}

fn parse_fetch_item_list(input: &str) -> Result<Vec<FetchItem>> {
    let source = strip_wrapping_parens(input.trim());
    let chars = source.chars().collect::<Vec<_>>();
    let mut cursor = 0usize;
    let mut items = Vec::new();

    while cursor < chars.len() {
        while cursor < chars.len() && chars[cursor].is_whitespace() {
            cursor += 1;
        }
        if cursor >= chars.len() {
            break;
        }

        let start = cursor;
        let mut bracket_depth = 0usize;
        let mut paren_depth = 0usize;
        while cursor < chars.len() {
            match chars[cursor] {
                '[' => bracket_depth += 1,
                ']' => bracket_depth = bracket_depth.saturating_sub(1),
                '(' => paren_depth += 1,
                ')' if paren_depth > 0 => paren_depth -= 1,
                ch if ch.is_whitespace() && bracket_depth == 0 && paren_depth == 0 => break,
                _ => {}
            }
            cursor += 1;
        }

        items.push(parse_fetch_item(
            &chars[start..cursor].iter().collect::<String>(),
        )?);
    }

    Ok(items)
}

fn strip_wrapping_parens(value: &str) -> &str {
    let trimmed = value.trim();
    if !(trimmed.starts_with('(') && trimmed.ends_with(')')) {
        return trimmed;
    }

    let mut depth = 0usize;
    for (index, ch) in trimmed.char_indices() {
        match ch {
            '(' => depth += 1,
            ')' => {
                depth = depth.saturating_sub(1);
                if depth == 0 && index != trimmed.len() - 1 {
                    return trimmed;
                }
            }
            _ => {}
        }
    }
    &trimmed[1..trimmed.len() - 1]
}

fn parse_fetch_item(raw: &str) -> Result<FetchItem> {
    let upper = raw.to_ascii_uppercase();
    Ok(match upper.as_str() {
        "UID" => FetchItem::Uid,
        "FLAGS" => FetchItem::Flags,
        "MODSEQ" => FetchItem::Modseq,
        "INTERNALDATE" => FetchItem::InternalDate,
        "RFC822.SIZE" => FetchItem::Rfc822Size,
        "ENVELOPE" => FetchItem::Envelope,
        "BODY" => FetchItem::Body,
        "BODYSTRUCTURE" => FetchItem::BodyStructure,
        "RFC822" => FetchItem::BodySection(BodySectionFetch {
            peek: false,
            section: String::new(),
            partial: None,
            response_label: Some("RFC822".to_string()),
        }),
        "RFC822.PEEK" => FetchItem::BodySection(BodySectionFetch {
            peek: true,
            section: String::new(),
            partial: None,
            response_label: Some("RFC822.PEEK".to_string()),
        }),
        "RFC822.HEADER" => FetchItem::BodySection(BodySectionFetch {
            peek: true,
            section: "HEADER".to_string(),
            partial: None,
            response_label: Some("RFC822.HEADER".to_string()),
        }),
        "RFC822.TEXT" => FetchItem::BodySection(BodySectionFetch {
            peek: false,
            section: "TEXT".to_string(),
            partial: None,
            response_label: Some("RFC822.TEXT".to_string()),
        }),
        _ => parse_body_fetch_item(raw)?,
    })
}

fn parse_body_fetch_item(raw: &str) -> Result<FetchItem> {
    let upper = raw.to_ascii_uppercase();
    let (peek, rest) = if upper.starts_with("BODY.PEEK[") {
        (true, &raw["BODY.PEEK".len()..])
    } else if upper.starts_with("BODY[") {
        (false, &raw["BODY".len()..])
    } else {
        bail!("unsupported FETCH attribute {}", raw);
    };

    let close = rest
        .find(']')
        .ok_or_else(|| anyhow!("invalid BODY section"))?;
    let section = rest[1..close].trim().to_ascii_uppercase();
    let partial = parse_partial_range(rest[close + 1..].trim())?;
    Ok(FetchItem::BodySection(BodySectionFetch {
        peek,
        section,
        partial,
        response_label: None,
    }))
}

fn parse_partial_range(value: &str) -> Result<Option<PartialRange>> {
    if value.is_empty() {
        return Ok(None);
    }
    let inner = value
        .strip_prefix('<')
        .and_then(|value| value.strip_suffix('>'))
        .ok_or_else(|| anyhow!("invalid partial FETCH range"))?;
    let (start, length) = inner
        .split_once('.')
        .ok_or_else(|| anyhow!("invalid partial FETCH range"))?;
    Ok(Some(PartialRange {
        start: start.parse()?,
        length: length.parse()?,
    }))
}

fn fetch_item_marks_seen(item: &FetchItem) -> bool {
    match item {
        FetchItem::BodySection(section) if !section.peek => {
            let normalized = normalize_body_section(&section.section);
            matches!(normalized.as_str(), "" | "TEXT" | "1" | "1.TEXT")
        }
        _ => false,
    }
}

fn append_body_section(output: &mut Vec<u8>, email: &ImapEmail, section: &BodySectionFetch) {
    let normalized = normalize_body_section(&section.section);
    let value = match normalized.as_str() {
        "HEADER" | "0" | "0.HEADER" => render_header(email),
        value if is_header_fields_section(value) => render_header_fields(email, value),
        "TEXT" => render_message_body(email),
        "" => render_full_message(email),
        "MIME" | "0.MIME" => render_root_mime_header(email),
        _ => render_part_section(email, &normalized),
    };
    let (partial_start, bytes) = apply_partial(value.as_bytes(), section.partial);
    append_literal(output, &section_label(section, partial_start), bytes);
}

fn normalize_body_section(section: &str) -> String {
    section.trim().to_ascii_uppercase()
}

fn is_header_fields_section(section: &str) -> bool {
    section.starts_with("HEADER.FIELDS")
        || section
            .split_once('.')
            .is_some_and(|(_, rest)| rest.starts_with("HEADER.FIELDS"))
}

fn section_label(section: &BodySectionFetch, partial_start: Option<usize>) -> String {
    let mut label = section
        .response_label
        .clone()
        .unwrap_or_else(|| format!("BODY[{}]", section.section));
    if let Some(start) = partial_start {
        label.push_str(&format!("<{}>", start));
    }
    label
}

fn apply_partial(value: &[u8], partial: Option<PartialRange>) -> (Option<usize>, &[u8]) {
    let Some(partial) = partial else {
        return (None, value);
    };
    let start = partial.start.min(value.len());
    let end = start.saturating_add(partial.length).min(value.len());
    (Some(partial.start), &value[start..end])
}

fn append_literal(output: &mut Vec<u8>, label: &str, value: &[u8]) {
    output.extend_from_slice(format!("{} {{{}}}\r\n", label, value.len()).as_bytes());
    output.extend_from_slice(value);
}

pub(crate) fn render_flags(email: &ImapEmail, mailbox_name: &str) -> String {
    let mut flags = Vec::new();
    if !email.unread {
        flags.push("\\Seen".to_string());
    }
    if email.flagged {
        flags.push("\\Flagged".to_string());
    }
    if email.deleted {
        flags.push("\\Deleted".to_string());
    }
    if mailbox_name.eq_ignore_ascii_case("Drafts") {
        flags.push("\\Draft".to_string());
    }
    for keyword in email.keywords.iter().filter_map(|keyword| {
        let keyword = keyword.trim();
        imap_keyword_atom(keyword).map(str::to_string)
    }) {
        if !flags.contains(&keyword) {
            flags.push(keyword);
        }
    }
    flags.join(" ")
}

fn imap_keyword_atom(keyword: &str) -> Option<&str> {
    if keyword.is_empty() || keyword.starts_with('\\') {
        return None;
    }
    keyword
        .bytes()
        .all(|byte| (0x21..=0x7e).contains(&byte) && !b"(){ %*\"\\]".contains(&byte))
        .then_some(keyword)
}

pub(crate) fn render_status_response(
    mailbox_name: &str,
    emails: &[ImapEmail],
    requested: &[String],
    state: &ImapMailboxState,
    utf8_enabled: bool,
) -> String {
    let unseen = emails.iter().filter(|email| email.unread).count();
    let items = requested
        .iter()
        .map(|item| match item.as_str() {
            "MESSAGES" => format!("MESSAGES {}", emails.len()),
            "RECENT" => "RECENT 0".to_string(),
            "UIDNEXT" => format!("UIDNEXT {}", state.uid_next),
            "UIDVALIDITY" => format!("UIDVALIDITY {}", state.uid_validity),
            "UNSEEN" => format!("UNSEEN {}", unseen),
            "HIGHESTMODSEQ" => format!("HIGHESTMODSEQ {}", state.highest_modseq),
            _ => format!("{} 0", item),
        })
        .collect::<Vec<_>>()
        .join(" ");
    format!(
        "* STATUS {} ({})\r\n",
        render_imap_mailbox_response_path(mailbox_name, utf8_enabled),
        items
    )
}

fn render_header(email: &ImapEmail) -> String {
    render_header_lines(email).join("\r\n") + "\r\n\r\n"
}

fn render_header_lines(email: &ImapEmail) -> Vec<String> {
    let mut lines = Vec::new();
    lines.push(format!("Date: {}", format_message_date(email)));
    lines.push(format!(
        "From: {}",
        render_address_header(
            normalized_display_name(email.from_display.as_deref(), &email.from_address),
            fallback_address(&email.from_address),
        )
    ));
    if !email.to.is_empty() {
        lines.push(format!("To: {}", render_recipient_header(&email.to)));
    }
    if !email.cc.is_empty() {
        lines.push(format!("Cc: {}", render_recipient_header(&email.cc)));
    }
    lines.push(format!("Subject: {}", email.subject));
    if let Some(message_id) = email.internet_message_id.as_deref() {
        lines.push(format!("Message-Id: {}", message_id));
    }
    lines.push("MIME-Version: 1.0".to_string());
    lines.push(format!("Content-Type: {}", root_content_type(email)));
    lines
}

fn render_header_fields(email: &ImapEmail, section: &str) -> String {
    let excluded = section.starts_with("HEADER.FIELDS.NOT")
        || section
            .split_once('.')
            .is_some_and(|(_, rest)| rest.starts_with("HEADER.FIELDS.NOT"));
    let requested = section
        .split_once('(')
        .and_then(|(_, rest)| rest.rsplit_once(')').map(|(fields, _)| fields))
        .unwrap_or_default()
        .split_whitespace()
        .map(|field| field.trim_end_matches(':').to_ascii_lowercase())
        .collect::<HashSet<_>>();
    if requested.is_empty() {
        return "\r\n".to_string();
    }

    let lines = render_header_lines(email)
        .into_iter()
        .filter(|line| {
            line.split_once(':')
                .map(|(name, _)| {
                    let contains = requested.contains(&name.to_ascii_lowercase());
                    if excluded {
                        !contains
                    } else {
                        contains
                    }
                })
                .unwrap_or(false)
        })
        .collect::<Vec<_>>();
    if lines.is_empty() {
        "\r\n".to_string()
    } else {
        lines.join("\r\n") + "\r\n\r\n"
    }
}

fn render_full_message(email: &ImapEmail) -> String {
    format!("{}{}", render_header(email), render_message_body(email))
}

fn message_rfc822_size(email: &ImapEmail) -> usize {
    render_full_message(email).as_bytes().len()
}

fn render_message_body(email: &ImapEmail) -> String {
    if email_has_attachment_parts(email) || email.has_attachments {
        return render_mixed_body(email);
    }
    if email.body_html_sanitized.is_some() {
        return render_alternative_body(email);
    }
    email.body_text.clone()
}

fn render_part_section(email: &ImapEmail, section: &str) -> String {
    let (part_path, suffix) = section
        .split_once('.')
        .map(|(part_path, suffix)| (part_path, suffix))
        .unwrap_or((section, ""));
    if part_path.is_empty() {
        return email.body_text.clone();
    }

    if suffix == "MIME" {
        return render_part_mime_header(email, part_path);
    }
    if suffix == "HEADER" || suffix.starts_with("HEADER.FIELDS") {
        return render_header(email);
    }

    match resolve_body_part(email, part_path) {
        Some(ResolvedBodyPart::Plain) => email.body_text.clone(),
        Some(ResolvedBodyPart::Html) => email
            .body_html_sanitized
            .as_deref()
            .unwrap_or(&email.body_text)
            .to_string(),
        Some(ResolvedBodyPart::Attachment(_)) => String::new(),
        None => email.body_text.clone(),
    }
}

fn render_text_part_mime_header(subtype: &str, charset: &str) -> String {
    format!(
        "Content-Type: text/{subtype}; charset={charset}\r\nContent-Transfer-Encoding: 7bit\r\n\r\n"
    )
}

fn render_root_mime_header(email: &ImapEmail) -> String {
    format!(
        "MIME-Version: 1.0\r\nContent-Type: {}\r\n\r\n",
        root_content_type(email)
    )
}

fn root_content_type(email: &ImapEmail) -> String {
    if email_has_attachment_parts(email) || email.has_attachments {
        format!("multipart/mixed; boundary=\"{}\"", mixed_boundary(email))
    } else if email.body_html_sanitized.is_some() {
        format!(
            "multipart/alternative; boundary=\"{}\"",
            multipart_boundary(email)
        )
    } else {
        "text/plain; charset=UTF-8".to_string()
    }
}

fn render_alternative_body(email: &ImapEmail) -> String {
    let boundary = multipart_boundary(email);
    let html = email
        .body_html_sanitized
        .as_deref()
        .unwrap_or(&email.body_text);
    format!(
        concat!(
            "--{boundary}\r\n",
            "Content-Type: text/plain; charset={text_charset}\r\n",
            "Content-Transfer-Encoding: 7bit\r\n",
            "\r\n",
            "{text}\r\n",
            "--{boundary}\r\n",
            "Content-Type: text/html; charset={html_charset}\r\n",
            "Content-Transfer-Encoding: 7bit\r\n",
            "\r\n",
            "{html}\r\n",
            "--{boundary}--\r\n"
        ),
        boundary = boundary,
        text_charset = body_part_charset(email, "1", "UTF-8"),
        html_charset = body_part_charset(email, "2", "UTF-8"),
        text = email.body_text,
        html = html
    )
}

fn render_mixed_body(email: &ImapEmail) -> String {
    let boundary = mixed_boundary(email);
    let mut body = String::new();
    if email.body_html_sanitized.is_some() {
        body.push_str(&format!(
            "--{boundary}\r\nContent-Type: multipart/alternative; boundary=\"{}\"\r\n\r\n",
            multipart_boundary(email)
        ));
        body.push_str(&render_alternative_body(email));
    } else {
        body.push_str(&format!(
            "--{boundary}\r\n{}{}",
            render_text_part_mime_header("plain", &body_part_charset(email, "1", "UTF-8")),
            email.body_text
        ));
        body.push_str("\r\n");
    }

    for part in attachment_parts(email) {
        body.push_str(&format!(
            "--{boundary}\r\n{}",
            render_attachment_mime_header(part)
        ));
    }
    if email.has_attachments && attachment_parts(email).is_empty() {
        body.push_str(&format!(
            "--{boundary}\r\nContent-Type: application/octet-stream\r\nContent-Transfer-Encoding: base64\r\nContent-Disposition: attachment\r\n\r\n"
        ));
    }
    body.push_str(&format!("--{boundary}--\r\n"));
    body
}

fn multipart_boundary(email: &ImapEmail) -> String {
    format!("lpe-alt-{}", email.id)
}

fn mixed_boundary(email: &ImapEmail) -> String {
    format!("lpe-mixed-{}", email.id)
}

fn render_envelope(email: &ImapEmail) -> String {
    let from_address = fallback_address(&email.from_address);
    let from_display = normalized_display_name(email.from_display.as_deref(), from_address);
    format!(
        "({} {} {} {} {} {} {} {} {} {})",
        nstring(Some(&format_message_date(email))),
        nstring(Some(&email.subject)),
        render_address_list(from_display, Some(from_address)),
        render_address_list(from_display, Some(from_address)),
        render_address_list(from_display, Some(from_address)),
        render_recipients(&email.to),
        render_recipients(&email.cc),
        "NIL",
        "NIL",
        nstring(email.internet_message_id.as_deref()),
    )
}

fn render_recipients(recipients: &[JmapEmailAddress]) -> String {
    if recipients.is_empty() {
        return "NIL".to_string();
    }
    format!(
        "({})",
        recipients
            .iter()
            .map(|recipient| {
                render_single_address(
                    normalized_display_name(recipient.display_name.as_deref(), &recipient.address),
                    fallback_address(&recipient.address),
                )
            })
            .collect::<Vec<_>>()
            .join(" ")
    )
}

fn render_address_list(display_name: Option<&str>, address: Option<&str>) -> String {
    let Some(address) = address.filter(|value| !value.trim().is_empty()) else {
        return "NIL".to_string();
    };
    format!(
        "({})",
        render_single_address(
            normalized_display_name(display_name, address),
            address.trim()
        )
    )
}

fn render_single_address(display_name: Option<&str>, address: &str) -> String {
    let address = fallback_address(address);
    let (mailbox, host) = address.split_once('@').unwrap_or((address, "localhost"));
    format!(
        "({} NIL {} {})",
        nstring(display_name),
        nstring(Some(mailbox)),
        nstring(Some(host))
    )
}

fn render_bodystructure(email: &ImapEmail) -> String {
    let body = render_body_bodystructure(email);
    let attachment_parts = attachment_parts(email);
    if !attachment_parts.is_empty() {
        format!(
            "({} {} \"MIXED\" (\"BOUNDARY\" \"{}\") NIL NIL)",
            body,
            attachment_parts
                .iter()
                .map(|part| render_attachment_bodystructure(part))
                .collect::<Vec<_>>()
                .join(" "),
            mixed_boundary(email)
        )
    } else if email.has_attachments {
        format!(
            "({} {} \"MIXED\" (\"BOUNDARY\" \"{}\") NIL NIL)",
            body,
            render_fallback_attachment_bodystructure(),
            mixed_boundary(email)
        )
    } else {
        body
    }
}

fn render_body_bodystructure(email: &ImapEmail) -> String {
    let text = render_text_bodystructure(
        &email.body_text,
        "PLAIN",
        &body_part_charset(email, "1", "UTF-8"),
    );
    if let Some(html) = email.body_html_sanitized.as_deref() {
        format!(
            "({} {} \"ALTERNATIVE\" (\"BOUNDARY\" \"{}\") NIL NIL)",
            text,
            render_text_bodystructure(html, "HTML", &body_part_charset(email, "2", "UTF-8")),
            multipart_boundary(email)
        )
    } else {
        text
    }
}

fn render_text_bodystructure(value: &str, subtype: &str, charset: &str) -> String {
    format!(
        "(\"TEXT\" \"{}\" (\"CHARSET\" {}) NIL NIL \"7BIT\" {} {} NIL NIL NIL)",
        subtype,
        nstring(Some(charset)),
        value.as_bytes().len(),
        value.lines().count().max(1)
    )
}

fn render_attachment_bodystructure(part: &ImapMimePart) -> String {
    let (type_name, subtype) = split_content_type(&part.content_type);
    let parameters = render_content_type_parameters(part);
    let encoding = part
        .transfer_encoding
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("BASE64")
        .to_ascii_uppercase();
    format!(
        "({} {} {} {} {} {} {} NIL {} NIL)",
        nstring(Some(&type_name)),
        nstring(Some(&subtype)),
        parameters,
        nstring(part.content_id.as_deref()),
        "NIL",
        nstring(Some(&encoding)),
        part.size_octets.max(0),
        render_disposition(part),
    )
}

fn render_fallback_attachment_bodystructure() -> String {
    "(\"APPLICATION\" \"OCTET-STREAM\" NIL NIL NIL \"BASE64\" 0 NIL (\"ATTACHMENT\" NIL) NIL)"
        .to_string()
}

fn email_has_attachment_parts(email: &ImapEmail) -> bool {
    email.mime_parts.iter().any(imap_attachment_part)
}

fn attachment_parts(email: &ImapEmail) -> Vec<&ImapMimePart> {
    email
        .mime_parts
        .iter()
        .filter(|part| imap_attachment_part(part))
        .collect()
}

fn imap_attachment_part(part: &ImapMimePart) -> bool {
    part.content_disposition
        .as_deref()
        .map(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "attachment" | "inline"
            )
        })
        .unwrap_or(false)
        || part
            .file_name
            .as_deref()
            .is_some_and(|value| !value.trim().is_empty())
        || part
            .content_id
            .as_deref()
            .is_some_and(|value| !value.trim().is_empty())
}

fn split_content_type(content_type: &str) -> (String, String) {
    let media_type = content_type
        .split(';')
        .next()
        .unwrap_or("application/octet-stream")
        .trim();
    let (type_name, subtype) = media_type
        .split_once('/')
        .unwrap_or(("application", "octet-stream"));
    (
        imap_media_token(type_name, "APPLICATION"),
        imap_media_token(subtype, "OCTET-STREAM"),
    )
}

fn imap_media_token(value: &str, fallback: &str) -> String {
    let value = value.trim();
    if value.is_empty() {
        fallback.to_string()
    } else {
        value.to_ascii_uppercase()
    }
}

fn render_content_type_parameters(part: &ImapMimePart) -> String {
    let mut parameters = Vec::new();
    if let Some(charset) = part
        .charset_name
        .as_deref()
        .or_else(|| content_type_parameter(&part.content_type, "charset"))
        .filter(|value| !value.trim().is_empty())
    {
        parameters.push(("CHARSET", charset.trim().to_string()));
    }
    if let Some(file_name) = part
        .file_name
        .as_deref()
        .filter(|value| !value.trim().is_empty())
    {
        parameters.push(("NAME", file_name.trim().to_string()));
    }
    if parameters.is_empty() {
        return "NIL".to_string();
    }
    format!(
        "({})",
        parameters
            .into_iter()
            .map(|(name, value)| format!("{} {}", nstring(Some(name)), nstring(Some(&value))))
            .collect::<Vec<_>>()
            .join(" ")
    )
}

fn content_type_parameter<'a>(content_type: &'a str, name: &str) -> Option<&'a str> {
    content_type.split(';').skip(1).find_map(|parameter| {
        let (key, value) = parameter.trim().split_once('=')?;
        if key.trim().eq_ignore_ascii_case(name) {
            Some(value.trim().trim_matches('"'))
        } else {
            None
        }
    })
}

fn render_part_mime_header(email: &ImapEmail, part_path: &str) -> String {
    match resolve_body_part(email, part_path) {
        Some(ResolvedBodyPart::Plain) => {
            render_text_part_mime_header("plain", &body_part_charset(email, "1", "UTF-8"))
        }
        Some(ResolvedBodyPart::Html) => {
            render_text_part_mime_header("html", &body_part_charset(email, "2", "UTF-8"))
        }
        Some(ResolvedBodyPart::Attachment(part)) => render_attachment_mime_header(part),
        None => render_text_part_mime_header("plain", &body_part_charset(email, "1", "UTF-8")),
    }
}

fn render_attachment_mime_header(part: &ImapMimePart) -> String {
    let mut lines = vec![format!("Content-Type: {}", part.content_type.trim())];
    lines.push(format!(
        "Content-Transfer-Encoding: {}",
        part.transfer_encoding
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .unwrap_or("base64")
    ));
    if let Some(content_id) = part
        .content_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        lines.push(format!(
            "Content-ID: <{}>",
            content_id.trim_matches(['<', '>'])
        ));
    }
    if let Some(disposition) = part
        .content_disposition
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        let mut value = disposition.to_string();
        if let Some(file_name) = part
            .file_name
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            value.push_str(&format!(
                "; filename=\"{}\"",
                sanitize_imap_quoted(file_name)
            ));
        }
        lines.push(format!("Content-Disposition: {}", value));
    }
    lines.join("\r\n") + "\r\n\r\n"
}

fn render_disposition(part: &ImapMimePart) -> String {
    let disposition = part
        .content_disposition
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("attachment")
        .to_ascii_uppercase();
    let parameters = part
        .file_name
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .map(|file_name| format!("(\"FILENAME\" {})", nstring(Some(file_name))))
        .unwrap_or_else(|| "NIL".to_string());
    format!("({} {})", nstring(Some(&disposition)), parameters)
}

enum ResolvedBodyPart<'a> {
    Plain,
    Html,
    Attachment(&'a ImapMimePart),
}

fn resolve_body_part<'a>(email: &'a ImapEmail, part_path: &str) -> Option<ResolvedBodyPart<'a>> {
    let normalized = part_path.trim();
    if normalized == "1" || normalized == "1.1" {
        return Some(ResolvedBodyPart::Plain);
    }
    if email.body_html_sanitized.is_some() && (normalized == "2" || normalized == "1.2") {
        return Some(ResolvedBodyPart::Html);
    }
    if let Some(part) = email
        .mime_parts
        .iter()
        .find(|part| part.part_path.eq_ignore_ascii_case(normalized))
    {
        return Some(ResolvedBodyPart::Attachment(part));
    }

    let attachment_index = normalized.parse::<usize>().ok()?.checked_sub(2)?;
    attachment_parts(email)
        .get(attachment_index)
        .copied()
        .map(ResolvedBodyPart::Attachment)
}

fn body_part_charset(email: &ImapEmail, path: &str, fallback: &str) -> String {
    email
        .mime_parts
        .iter()
        .find(|part| part.part_path == path)
        .and_then(|part| {
            part.charset_name
                .as_deref()
                .or_else(|| content_type_parameter(&part.content_type, "charset"))
        })
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or(fallback)
        .to_string()
}

fn nstring(value: Option<&str>) -> String {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| format!("\"{}\"", sanitize_imap_quoted(value)))
        .unwrap_or_else(|| "NIL".to_string())
}

pub(crate) fn render_visible_header(email: &ImapEmail) -> String {
    let mut lines = Vec::new();
    lines.push(format!(
        "Date: {}",
        email.sent_at.as_deref().unwrap_or(&email.received_at)
    ));
    lines.push(format!(
        "From: {}",
        render_address_header(email.from_display.as_deref(), &email.from_address)
    ));
    if !email.to.is_empty() {
        lines.push(format!("To: {}", render_recipient_header(&email.to)));
    }
    if !email.cc.is_empty() {
        lines.push(format!("Cc: {}", render_recipient_header(&email.cc)));
    }
    lines.push(format!("Subject: {}", email.subject));
    if let Some(message_id) = email.internet_message_id.as_deref() {
        lines.push(format!("Message-Id: {}", message_id));
    }
    lines.join("\r\n")
}

pub(crate) fn render_recipient_header(recipients: &[JmapEmailAddress]) -> String {
    recipients
        .iter()
        .map(|recipient| {
            render_address_header(recipient.display_name.as_deref(), &recipient.address)
        })
        .collect::<Vec<_>>()
        .join(", ")
}

pub(crate) fn render_address_header(display_name: Option<&str>, address: &str) -> String {
    let address = fallback_address(address);
    let display_name = normalized_display_name(display_name, address);
    match display_name
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        Some(display) => format!("{} <{}>", display, address),
        None => address.to_string(),
    }
}

fn fallback_address(address: &str) -> &str {
    let address = address.trim();
    if address.is_empty() {
        "unknown@localhost"
    } else {
        address
    }
}

fn normalized_display_name<'a>(display_name: Option<&'a str>, address: &str) -> Option<&'a str> {
    let display_name = display_name?.trim();
    if display_name.is_empty() || display_name.eq_ignore_ascii_case(address.trim()) {
        None
    } else {
        Some(display_name)
    }
}

fn format_internal_date(email: &ImapEmail) -> String {
    let source = email.sent_at.as_deref().unwrap_or(&email.received_at);
    let month = match &source[5..7] {
        "01" => "Jan",
        "02" => "Feb",
        "03" => "Mar",
        "04" => "Apr",
        "05" => "May",
        "06" => "Jun",
        "07" => "Jul",
        "08" => "Aug",
        "09" => "Sep",
        "10" => "Oct",
        "11" => "Nov",
        "12" => "Dec",
        _ => "Jan",
    };
    format!(
        "{}-{}-{} {} +0000",
        &source[8..10],
        month,
        &source[0..4],
        &source[11..19]
    )
}

fn format_message_date(email: &ImapEmail) -> String {
    let source = email.sent_at.as_deref().unwrap_or(&email.received_at);
    format_rfc5322_date(source).unwrap_or_else(|| source.to_string())
}

fn format_rfc5322_date(source: &str) -> Option<String> {
    if source.len() < 19 {
        return None;
    }
    let year = source.get(0..4)?;
    let month_number = source.get(5..7)?;
    let day = source.get(8..10)?;
    let time = source.get(11..19)?;
    if source.get(4..5)? != "-"
        || source.get(7..8)? != "-"
        || source.get(10..11)? != "T"
        || !year.chars().all(|ch| ch.is_ascii_digit())
        || !month_number.chars().all(|ch| ch.is_ascii_digit())
        || !day.chars().all(|ch| ch.is_ascii_digit())
        || !time.chars().all(|ch| ch.is_ascii_digit() || ch == ':')
    {
        return None;
    }
    let month = month_name(month_number)?;
    Some(format!("{day} {month} {year} {time} +0000"))
}

fn month_name(value: &str) -> Option<&'static str> {
    match value {
        "01" => Some("Jan"),
        "02" => Some("Feb"),
        "03" => Some("Mar"),
        "04" => Some("Apr"),
        "05" => Some("May"),
        "06" => Some("Jun"),
        "07" => Some("Jul"),
        "08" => Some("Aug"),
        "09" => Some("Sep"),
        "10" => Some("Oct"),
        "11" => Some("Nov"),
        "12" => Some("Dec"),
        _ => None,
    }
}

pub(crate) fn resolve_message_indexes(
    emails: &[ImapEmail],
    set_token: &str,
    ref_kind: MessageRefKind,
) -> Result<Vec<usize>> {
    let max_sequence = emails.len() as u32;
    let mut indexes = Vec::new();
    for segment in set_token.split(',') {
        let segment = segment.trim();
        if segment.is_empty() {
            continue;
        }
        if let Some((start, end)) = segment.split_once(':') {
            let start = resolve_set_value(start, emails, max_sequence, ref_kind)?;
            let end = resolve_set_value(end, emails, max_sequence, ref_kind)?;
            let (from, to) = if start <= end {
                (start, end)
            } else {
                (end, start)
            };
            for value in from..=to {
                if let Some(index) = find_message_index(emails, value, ref_kind) {
                    if !indexes.contains(&index) {
                        indexes.push(index);
                    }
                }
            }
        } else {
            let value = resolve_set_value(segment, emails, max_sequence, ref_kind)?;
            if let Some(index) = find_message_index(emails, value, ref_kind) {
                if !indexes.contains(&index) {
                    indexes.push(index);
                }
            }
        }
    }
    Ok(indexes)
}

fn resolve_set_value(
    token: &str,
    emails: &[ImapEmail],
    max_sequence: u32,
    ref_kind: MessageRefKind,
) -> Result<u32> {
    if token == "*" {
        return Ok(match ref_kind {
            MessageRefKind::Sequence => max_sequence,
            MessageRefKind::Uid => emails.last().map(|email| email.uid).unwrap_or(0),
        });
    }
    token.parse::<u32>().map_err(Into::into)
}

fn find_message_index(emails: &[ImapEmail], value: u32, ref_kind: MessageRefKind) -> Option<usize> {
    match ref_kind {
        MessageRefKind::Sequence => value
            .checked_sub(1)
            .map(|index| index as usize)
            .filter(|index| *index < emails.len()),
        MessageRefKind::Uid => emails.iter().position(|email| email.uid == value),
    }
}

pub(crate) fn parse_status_items(arguments: &str) -> Result<Vec<String>> {
    let tokens = tokenize(arguments)?;
    if tokens.len() < 2 {
        bail!("STATUS expects a mailbox name and item list");
    }
    let source = tokens[1..].join(" ");
    let requested = source
        .trim()
        .trim_start_matches('(')
        .trim_end_matches(')')
        .split_whitespace()
        .map(|item| item.to_ascii_uppercase())
        .collect::<Vec<_>>();
    if requested.is_empty() {
        bail!("STATUS expects at least one data item");
    }
    for item in &requested {
        if !matches!(
            item.as_str(),
            "MESSAGES" | "RECENT" | "UIDNEXT" | "UIDVALIDITY" | "UNSEEN" | "HIGHESTMODSEQ"
        ) {
            bail!("unsupported STATUS item {}", item);
        }
    }
    Ok(requested)
}

pub(crate) fn render_modified_set(
    selected: &SelectedMailbox,
    modified_ids: &[Uuid],
    ref_kind: MessageRefKind,
) -> String {
    let mut values = Vec::new();
    for (index, email) in selected.emails.iter().enumerate() {
        if !modified_ids.contains(&email.id) {
            continue;
        }
        values.push(match ref_kind {
            MessageRefKind::Sequence => (index + 1).to_string(),
            MessageRefKind::Uid => email.uid.to_string(),
        });
    }
    values.join(",")
}

pub(crate) fn sanitize_imap_text(value: &str) -> String {
    value.replace('\r', " ").replace('\n', " ")
}

pub(crate) fn sanitize_imap_quoted(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"")
}

pub(crate) fn render_selected_updates(
    previous: &SelectedMailbox,
    current: &SelectedMailbox,
) -> Result<String> {
    let mut output = String::new();
    let previous_ids = previous
        .emails
        .iter()
        .map(|email| email.id)
        .collect::<HashSet<_>>();

    let current_ids = current
        .emails
        .iter()
        .map(|email| email.id)
        .collect::<HashSet<_>>();
    let membership_changed = previous_ids != current_ids;
    let mut removed_sequences = previous
        .emails
        .iter()
        .enumerate()
        .filter_map(|(index, email)| (!current_ids.contains(&email.id)).then_some(index + 1))
        .collect::<Vec<_>>();
    removed_sequences.sort_unstable_by(|left, right| right.cmp(left));
    for sequence in removed_sequences {
        output.push_str(&format!("* {} EXPUNGE\r\n", sequence));
    }

    if previous.emails.len() != current.emails.len() || membership_changed {
        output.push_str(&format!("* {} EXISTS\r\n", current.emails.len()));
    }

    for (index, email) in current.emails.iter().enumerate() {
        if !previous_ids.contains(&email.id) {
            output.push_str(&format!(
                "* {} FETCH (UID {} FLAGS ({}) MODSEQ ({}))\r\n",
                index + 1,
                email.uid,
                render_flags(email, &current.mailbox_name),
                email.modseq
            ));
            continue;
        }
        let Some(previous_email) = previous
            .emails
            .iter()
            .find(|candidate| candidate.id == email.id)
        else {
            continue;
        };
        if previous_email.unread != email.unread
            || previous_email.flagged != email.flagged
            || previous_email.deleted != email.deleted
        {
            output.push_str(&format!(
                "* {} FETCH (FLAGS ({}) MODSEQ ({}))\r\n",
                index + 1,
                render_flags(email, &current.mailbox_name),
                email.modseq
            ));
        }
    }

    Ok(output)
}

pub(crate) fn first_unseen_sequence(selected: &SelectedMailbox) -> usize {
    selected
        .emails
        .iter()
        .position(|email| email.unread)
        .map(|index| index + 1)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests;
