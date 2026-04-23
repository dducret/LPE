use anyhow::{bail, Result};
use std::collections::HashSet;
use uuid::Uuid;

use lpe_storage::{ImapEmail, JmapEmailAddress, JmapMailbox};

use crate::{parse::tokenize, MessageRefKind, SelectedMailbox, UID_VALIDITY};

pub(crate) struct FetchAttributes {
    pub(crate) items: Vec<String>,
    pub(crate) mark_seen: bool,
}

pub(crate) fn mailbox_name_matches(display_name: &str, role: &str, requested: &str) -> bool {
    requested.eq_ignore_ascii_case(display_name)
        || (role == "inbox" && requested.eq_ignore_ascii_case("INBOX"))
}

pub(crate) fn render_list_flags(role: &str) -> &'static str {
    match role {
        "inbox" => "(\\Inbox)",
        "sent" => "(\\Sent)",
        "drafts" => "(\\Drafts)",
        _ => "()",
    }
}

pub(crate) fn parse_fetch_attributes(input: &str) -> Result<FetchAttributes> {
    let upper = input.trim().to_ascii_uppercase();
    let expanded = match upper.as_str() {
        "ALL" => vec![
            "FLAGS".to_string(),
            "INTERNALDATE".to_string(),
            "RFC822.SIZE".to_string(),
            "UID".to_string(),
        ],
        "FAST" => vec![
            "FLAGS".to_string(),
            "INTERNALDATE".to_string(),
            "RFC822.SIZE".to_string(),
        ],
        "FULL" => vec![
            "FLAGS".to_string(),
            "INTERNALDATE".to_string(),
            "RFC822.SIZE".to_string(),
            "BODY[]".to_string(),
            "UID".to_string(),
        ],
        _ => {
            let source = input.trim().trim_start_matches('(').trim_end_matches(')');
            source
                .split_whitespace()
                .map(|item| item.to_ascii_uppercase())
                .collect()
        }
    };
    if expanded.is_empty() {
        bail!("FETCH expects at least one attribute");
    }
    let mark_seen = expanded.iter().any(|item| {
        matches!(
            item.as_str(),
            "BODY[]" | "BODY[TEXT]" | "RFC822" | "RFC822.TEXT"
        )
    });
    Ok(FetchAttributes {
        items: expanded,
        mark_seen,
    })
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
        match item.as_str() {
            "UID" => output.extend_from_slice(format!("UID {}", email.uid).as_bytes()),
            "FLAGS" => output.extend_from_slice(
                format!("FLAGS ({})", render_flags(email, &email.mailbox_name)).as_bytes(),
            ),
            "MODSEQ" => output.extend_from_slice(format!("MODSEQ ({})", email.modseq).as_bytes()),
            "INTERNALDATE" => output.extend_from_slice(
                format!("INTERNALDATE \"{}\"", format_internal_date(email)).as_bytes(),
            ),
            "RFC822.SIZE" => output
                .extend_from_slice(format!("RFC822.SIZE {}", email.size_octets.max(0)).as_bytes()),
            "BODY[HEADER]" | "BODY.PEEK[HEADER]" => {
                append_literal(&mut output, item, render_header(email).as_bytes());
            }
            "BODY[TEXT]" | "BODY.PEEK[TEXT]" | "RFC822.TEXT" => {
                append_literal(&mut output, item, email.body_text.as_bytes());
            }
            "BODY[]" | "BODY.PEEK[]" | "RFC822" => {
                append_literal(&mut output, item, render_full_message(email).as_bytes());
            }
            other => bail!("unsupported FETCH attribute {}", other),
        }
    }
    output.extend_from_slice(b")\r\n");
    Ok(output)
}

fn append_literal(output: &mut Vec<u8>, label: &str, value: &[u8]) {
    output.extend_from_slice(format!("{} {{{}}}\r\n", label, value.len()).as_bytes());
    output.extend_from_slice(value);
}

pub(crate) fn render_flags(email: &ImapEmail, mailbox_name: &str) -> String {
    let mut flags = Vec::new();
    if !email.unread {
        flags.push("\\Seen");
    }
    if email.flagged {
        flags.push("\\Flagged");
    }
    if mailbox_name.eq_ignore_ascii_case("Drafts") {
        flags.push("\\Draft");
    }
    flags.join(" ")
}

pub(crate) fn render_status_response(
    mailbox: &JmapMailbox,
    emails: &[ImapEmail],
    requested: &[String],
    highest_modseq: u64,
) -> String {
    let uid_next = emails
        .last()
        .map(|email| email.uid.saturating_add(1))
        .unwrap_or(1);
    let unseen = emails.iter().filter(|email| email.unread).count();
    let items = requested
        .iter()
        .map(|item| match item.as_str() {
            "MESSAGES" => format!("MESSAGES {}", emails.len()),
            "RECENT" => "RECENT 0".to_string(),
            "UIDNEXT" => format!("UIDNEXT {}", uid_next),
            "UIDVALIDITY" => format!("UIDVALIDITY {}", UID_VALIDITY),
            "UNSEEN" => format!("UNSEEN {}", unseen),
            "HIGHESTMODSEQ" => format!("HIGHESTMODSEQ {}", highest_modseq),
            _ => format!("{} 0", item),
        })
        .collect::<Vec<_>>()
        .join(" ");
    format!(
        "* STATUS \"{}\" ({})\r\n",
        sanitize_imap_quoted(&mailbox.name),
        items
    )
}

fn render_header(email: &ImapEmail) -> String {
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
    if !email.bcc.is_empty() && matches!(email.mailbox_role.as_str(), "drafts" | "sent") {
        lines.push(format!("Bcc: {}", render_recipient_header(&email.bcc)));
    }
    lines.push(format!("Subject: {}", email.subject));
    if let Some(message_id) = email.internet_message_id.as_deref() {
        lines.push(format!("Message-Id: {}", message_id));
    }
    lines.join("\r\n") + "\r\n\r\n"
}

fn render_full_message(email: &ImapEmail) -> String {
    format!("{}{}", render_header(email), email.body_text)
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
    match display_name
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        Some(display) => format!("{} <{}>", display, address),
        None => address.to_string(),
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
                "* {} FETCH (UID {} FLAGS ({}))\r\n",
                index + 1,
                email.uid,
                render_flags(email, &current.mailbox_name)
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
        if previous_email.unread != email.unread || previous_email.flagged != email.flagged {
            output.push_str(&format!(
                "* {} FETCH (FLAGS ({}))\r\n",
                index + 1,
                render_flags(email, &current.mailbox_name)
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
