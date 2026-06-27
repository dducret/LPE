use crate::{days_from_civil, month_abbrev};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DisplayNamePolicy {
    Include,
    OmitIfEqualsAddress,
}

pub fn sanitize_header_value(value: &str) -> String {
    value
        .replace(['\r', '\n'], " ")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

pub fn quote_display_name(value: &str) -> String {
    let value = sanitize_header_value(value);
    if value
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, ' ' | '.' | '_' | '-'))
    {
        value
    } else {
        format!("\"{}\"", quote_header_parameter(&value))
    }
}

pub fn quote_header_parameter(value: &str) -> String {
    sanitize_header_value(value)
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
}

pub fn format_mailbox_address(
    address: &str,
    display_name: Option<&str>,
    policy: DisplayNamePolicy,
) -> String {
    let address = sanitize_header_value(address);
    match display_name
        .map(sanitize_header_value)
        .filter(|value| !value.trim().is_empty())
        .filter(|value| policy == DisplayNamePolicy::Include || value != &address)
    {
        Some(display_name) => format!("{} <{}>", quote_display_name(&display_name), address),
        None => address,
    }
}

pub fn normalize_mime_body(value: &str) -> String {
    let normalized = value.replace("\r\n", "\n").replace('\r', "\n");
    let mut normalized = normalized.replace('\n', "\r\n");
    if !normalized.ends_with("\r\n") {
        normalized.push_str("\r\n");
    }
    normalized
}

pub fn rfc5322_utc_date(value: &str) -> String {
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
    let days = days_from_civil(i64::from(year), i64::from(month), i64::from(day));
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
    u8::try_from(month)
        .ok()
        .and_then(month_abbrev)
        .unwrap_or("Dec")
}
