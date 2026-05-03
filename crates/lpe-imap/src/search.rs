use anyhow::{anyhow, bail, Result};

use lpe_storage::{ImapEmail, JmapEmailAddress};

use crate::{
    parse::tokenize,
    render::{render_address_header, render_recipient_header, render_visible_header},
    MessageRefKind,
};

pub(crate) enum SearchExpression {
    All,
    Seen(bool),
    Flagged(bool),
    Deleted(bool),
    Answered(bool),
    Draft(bool),
    Recent(bool),
    Keyword(bool),
    Text(String),
    Subject(String),
    From(String),
    To(String),
    Cc(String),
    Body(String),
    Header(String, String),
    Before(i32),
    On(i32),
    Since(i32),
    Larger(i64),
    Smaller(i64),
    MessageSet(String, MessageRefKind),
    Not(Box<SearchExpression>),
    Or(Box<SearchExpression>, Box<SearchExpression>),
    And(Vec<SearchExpression>),
}

impl SearchExpression {
    pub(crate) fn from_tokens(tokens: &[String]) -> Result<Self> {
        if tokens.is_empty() {
            return Ok(Self::All);
        }

        let mut cursor = 0usize;
        let mut expressions = Vec::new();
        while cursor < tokens.len() {
            expressions.push(parse_search_key(tokens, &mut cursor)?);
        }

        if expressions.len() == 1 {
            Ok(expressions.pop().unwrap())
        } else {
            Ok(Self::And(expressions))
        }
    }

    pub(crate) fn matches(
        &self,
        email: &ImapEmail,
        index: usize,
        emails: &[ImapEmail],
        ref_kind: MessageRefKind,
    ) -> Result<bool> {
        Ok(match self {
            Self::All => true,
            Self::Seen(expected) => !email.unread == *expected,
            Self::Flagged(expected) => email.flagged == *expected,
            Self::Deleted(expected) => email.deleted == *expected,
            Self::Answered(expected) => !*expected,
            Self::Draft(expected) => (email.mailbox_role == "drafts") == *expected,
            Self::Recent(expected) => !*expected,
            Self::Keyword(expected) => !*expected,
            Self::Text(query) => {
                let query = normalize_search_text(query);
                search_email_text(email).contains(&query)
            }
            Self::Subject(query) => {
                normalize_search_text(&email.subject).contains(&normalize_search_text(query))
            }
            Self::From(query) => searchable_sender(email).contains(&normalize_search_text(query)),
            Self::To(query) => {
                searchable_recipients(&email.to).contains(&normalize_search_text(query))
            }
            Self::Cc(query) => {
                searchable_recipients(&email.cc).contains(&normalize_search_text(query))
            }
            Self::Body(query) => {
                normalize_search_text(&email.body_text).contains(&normalize_search_text(query))
            }
            Self::Header(name, query) => {
                searchable_header_value(email, name).contains(&normalize_search_text(query))
            }
            Self::Before(date) => message_search_date(email)? < *date,
            Self::On(date) => message_search_date(email)? == *date,
            Self::Since(date) => message_search_date(email)? >= *date,
            Self::Larger(size) => email.size_octets > *size,
            Self::Smaller(size) => email.size_octets < *size,
            Self::MessageSet(set, criterion_kind) => {
                let evaluation_kind = match criterion_kind {
                    MessageRefKind::Uid => MessageRefKind::Uid,
                    MessageRefKind::Sequence => ref_kind,
                };
                message_matches_set(email, index, emails, set, evaluation_kind)?
            }
            Self::Not(expression) => !expression.matches(email, index, emails, ref_kind)?,
            Self::Or(left, right) => {
                left.matches(email, index, emails, ref_kind)?
                    || right.matches(email, index, emails, ref_kind)?
            }
            Self::And(expressions) => expressions.iter().all(|expression| {
                expression
                    .matches(email, index, emails, ref_kind)
                    .unwrap_or(false)
            }),
        })
    }
}

fn message_matches_set(
    email: &ImapEmail,
    index: usize,
    emails: &[ImapEmail],
    set_token: &str,
    ref_kind: MessageRefKind,
) -> Result<bool> {
    let max_value = match ref_kind {
        MessageRefKind::Sequence => emails.len() as u32,
        MessageRefKind::Uid => emails.last().map(|candidate| candidate.uid).unwrap_or(0),
    };
    let value = match ref_kind {
        MessageRefKind::Sequence => (index + 1) as u32,
        MessageRefKind::Uid => email.uid,
    };

    for segment in set_token.split(',') {
        let segment = segment.trim();
        if segment.is_empty() {
            continue;
        }
        if let Some((start, end)) = segment.split_once(':') {
            let start = resolve_set_value(start, emails, max_value, ref_kind)?;
            let end = resolve_set_value(end, emails, max_value, ref_kind)?;
            let (from, to) = if start <= end {
                (start, end)
            } else {
                (end, start)
            };
            if value >= from && value <= to {
                return Ok(true);
            }
        } else if value == resolve_set_value(segment, emails, max_value, ref_kind)? {
            return Ok(true);
        }
    }

    Ok(false)
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

fn parse_search_key(tokens: &[String], cursor: &mut usize) -> Result<SearchExpression> {
    let token = tokens
        .get(*cursor)
        .ok_or_else(|| anyhow!("unexpected end of SEARCH criteria"))?
        .clone();
    *cursor += 1;

    if token.starts_with('(') && token.ends_with(')') && token.len() >= 2 {
        return SearchExpression::from_tokens(&tokenize(&token[1..token.len() - 1])?);
    }

    Ok(match token.to_ascii_uppercase().as_str() {
        "ALL" => SearchExpression::All,
        "SEEN" => SearchExpression::Seen(true),
        "UNSEEN" => SearchExpression::Seen(false),
        "FLAGGED" => SearchExpression::Flagged(true),
        "UNFLAGGED" => SearchExpression::Flagged(false),
        "DELETED" => SearchExpression::Deleted(true),
        "UNDELETED" => SearchExpression::Deleted(false),
        "ANSWERED" => SearchExpression::Answered(true),
        "UNANSWERED" => SearchExpression::Answered(false),
        "DRAFT" => SearchExpression::Draft(true),
        "UNDRAFT" => SearchExpression::Draft(false),
        "RECENT" | "NEW" => SearchExpression::Recent(true),
        "OLD" => SearchExpression::Recent(false),
        "KEYWORD" => {
            let _ = next_search_argument(tokens, cursor, "KEYWORD")?;
            SearchExpression::Keyword(true)
        }
        "UNKEYWORD" => {
            let _ = next_search_argument(tokens, cursor, "UNKEYWORD")?;
            SearchExpression::Keyword(false)
        }
        "TEXT" => SearchExpression::Text(next_search_argument(tokens, cursor, "TEXT")?),
        "SUBJECT" => SearchExpression::Subject(next_search_argument(tokens, cursor, "SUBJECT")?),
        "FROM" => SearchExpression::From(next_search_argument(tokens, cursor, "FROM")?),
        "TO" => SearchExpression::To(next_search_argument(tokens, cursor, "TO")?),
        "CC" => SearchExpression::Cc(next_search_argument(tokens, cursor, "CC")?),
        "BODY" => SearchExpression::Body(next_search_argument(tokens, cursor, "BODY")?),
        "HEADER" => SearchExpression::Header(
            next_search_argument(tokens, cursor, "HEADER")?,
            next_search_argument(tokens, cursor, "HEADER")?,
        ),
        "BEFORE" | "SENTBEFORE" => SearchExpression::Before(parse_search_date(
            &next_search_argument(tokens, cursor, "BEFORE")?,
        )?),
        "ON" | "SENTON" => SearchExpression::On(parse_search_date(&next_search_argument(
            tokens, cursor, "ON",
        )?)?),
        "SINCE" | "SENTSINCE" => SearchExpression::Since(parse_search_date(
            &next_search_argument(tokens, cursor, "SINCE")?,
        )?),
        "LARGER" => SearchExpression::Larger(
            next_search_argument(tokens, cursor, "LARGER")?.parse::<i64>()?,
        ),
        "SMALLER" => SearchExpression::Smaller(
            next_search_argument(tokens, cursor, "SMALLER")?.parse::<i64>()?,
        ),
        "UID" => SearchExpression::MessageSet(
            next_search_argument(tokens, cursor, "UID")?,
            MessageRefKind::Uid,
        ),
        "NOT" => SearchExpression::Not(Box::new(parse_search_key(tokens, cursor)?)),
        "OR" => SearchExpression::Or(
            Box::new(parse_search_key(tokens, cursor)?),
            Box::new(parse_search_key(tokens, cursor)?),
        ),
        other if looks_like_message_set(other) => {
            SearchExpression::MessageSet(token, MessageRefKind::Sequence)
        }
        other => bail!("unsupported SEARCH criterion {}", other),
    })
}

fn next_search_argument(tokens: &[String], cursor: &mut usize, criterion: &str) -> Result<String> {
    let value = tokens
        .get(*cursor)
        .cloned()
        .ok_or_else(|| anyhow!("SEARCH {} requires an argument", criterion))?;
    *cursor += 1;
    Ok(value)
}

fn looks_like_message_set(token: &str) -> bool {
    !token.is_empty()
        && token
            .chars()
            .all(|character| character.is_ascii_digit() || matches!(character, '*' | ':' | ','))
}

fn normalize_search_text(value: &str) -> String {
    value.to_ascii_lowercase()
}

fn searchable_sender(email: &ImapEmail) -> String {
    normalize_search_text(&render_address_header(
        email.from_display.as_deref(),
        &email.from_address,
    ))
}

fn searchable_recipients(recipients: &[JmapEmailAddress]) -> String {
    normalize_search_text(&render_recipient_header(recipients))
}

fn search_email_text(email: &ImapEmail) -> String {
    normalize_search_text(&format!(
        "{}\n{}\n{}",
        render_visible_header(email),
        email.body_text,
        email.preview
    ))
}

fn searchable_header_value(email: &ImapEmail, name: &str) -> String {
    match name.trim().to_ascii_uppercase().as_str() {
        "FROM" => searchable_sender(email),
        "TO" => searchable_recipients(&email.to),
        "CC" => searchable_recipients(&email.cc),
        "BCC" => String::new(),
        "SUBJECT" => normalize_search_text(&email.subject),
        "DATE" => normalize_search_text(email.sent_at.as_deref().unwrap_or(&email.received_at)),
        "MESSAGE-ID" => {
            normalize_search_text(email.internet_message_id.as_deref().unwrap_or_default())
        }
        _ => search_email_text(email),
    }
}

fn parse_search_date(value: &str) -> Result<i32> {
    let parts = value.split('-').collect::<Vec<_>>();
    if parts.len() != 3 {
        bail!("invalid SEARCH date {}", value);
    }
    let day = parts[0].trim().parse::<i32>()?;
    let month = match parts[1].trim().to_ascii_lowercase().as_str() {
        "jan" => 1,
        "feb" => 2,
        "mar" => 3,
        "apr" => 4,
        "may" => 5,
        "jun" => 6,
        "jul" => 7,
        "aug" => 8,
        "sep" => 9,
        "oct" => 10,
        "nov" => 11,
        "dec" => 12,
        _ => bail!("invalid SEARCH month {}", value),
    };
    let year = parts[2].trim().parse::<i32>()?;
    Ok((year * 10_000) + (month * 100) + day)
}

fn message_search_date(email: &ImapEmail) -> Result<i32> {
    let source = email.sent_at.as_deref().unwrap_or(&email.received_at);
    if source.len() < 10 {
        bail!("invalid message date");
    }
    let year = source[0..4].parse::<i32>()?;
    let month = source[5..7].parse::<i32>()?;
    let day = source[8..10].parse::<i32>()?;
    Ok((year * 10_000) + (month * 100) + day)
}
