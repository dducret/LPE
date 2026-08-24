use anyhow::Result;
use lpe_magika::{collect_mime_attachment_parts, extract_visible_body_parts};
use sqlx::types::chrono::{DateTime, FixedOffset, NaiveDate, TimeZone, Utc};
use std::collections::HashMap;
use uuid::Uuid;

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
    pub body_html_sanitized: Option<String>,
    pub attachments: Vec<AttachmentUploadInput>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedRfc822Header {
    pub name: String,
    pub value: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CalendarMeetingResponse {
    pub method: String,
    pub attendee_email: String,
    pub attendee_name: String,
    pub partstat: String,
    pub uid: String,
    pub response_sent_at: Option<String>,
    pub meeting_start: Option<String>,
    pub meeting_end: Option<String>,
    pub meeting_location: Option<String>,
    pub meeting_sequence: Option<i32>,
    pub proposed_start: Option<String>,
    pub proposed_end: Option<String>,
    pub original_start: Option<String>,
    pub original_end: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CalendarMeetingRequest {
    pub uid: String,
    pub transport_attachment_id: Option<Uuid>,
    pub response_requested: bool,
    pub sent_at: Option<String>,
    pub meeting_start: String,
    pub meeting_end: String,
    pub meeting_location: Option<String>,
    pub meeting_sequence: i32,
    pub intended_busy_status: i32,
}

pub fn normalize_calendar_meeting_uid(value: &str) -> String {
    let value = value.trim();
    let Some(encoded) = value
        .get(.."mapi-goid:".len())
        .filter(|prefix| prefix.eq_ignore_ascii_case("mapi-goid:"))
        .and_then(|_| value.get("mapi-goid:".len()..))
    else {
        return value.to_string();
    };
    if encoded.len() % 2 == 0 && encoded.as_bytes().iter().all(u8::is_ascii_hexdigit) {
        format!("mapi-goid:{}", encoded.to_ascii_lowercase())
    } else {
        value.to_string()
    }
}

pub fn parse_calendar_meeting_response(
    attachments: &[AttachmentUploadInput],
) -> Option<CalendarMeetingResponse> {
    attachments.iter().find_map(|attachment| {
        attachment
            .media_type
            .trim()
            .to_ascii_lowercase()
            .starts_with("text/calendar")
            .then(|| parse_icalendar_meeting_response(&attachment.blob_bytes))
            .flatten()
    })
}

pub fn parse_calendar_meeting_request(
    attachments: &[AttachmentUploadInput],
) -> Option<CalendarMeetingRequest> {
    // [MS-OXCMAIL] section 2.2.3.3.2 and [MS-OXCICAL] section 2.1.3.1.1.1:
    // the decoded text/calendar METHOD, not a legacy outer Content-Class, owns classification.
    attachments.iter().find_map(|attachment| {
        if !attachment
            .media_type
            .trim()
            .to_ascii_lowercase()
            .starts_with("text/calendar")
        {
            return None;
        }
        let declared_method = attachment
            .media_type
            .split(';')
            .skip(1)
            .find_map(|parameter| {
                let (name, value) = parameter.split_once('=')?;
                name.trim()
                    .eq_ignore_ascii_case("method")
                    .then(|| value.trim().trim_matches('"').to_ascii_uppercase())
            });
        if declared_method
            .as_deref()
            .is_some_and(|method| method != "REQUEST")
        {
            return None;
        }
        let lines = unfold_icalendar_lines(&String::from_utf8_lossy(&attachment.blob_bytes));
        if !icalendar_value(&lines, "METHOD")
            .is_some_and(|method| method.eq_ignore_ascii_case("REQUEST"))
            || lines
                .iter()
                .filter(|line| line.eq_ignore_ascii_case("BEGIN:VEVENT"))
                .count()
                != 1
            || lines
                .iter()
                .filter(|line| line.eq_ignore_ascii_case("END:VEVENT"))
                .count()
                != 1
        {
            return None;
        }
        parse_icalendar_meeting_request(&lines)
    })
}

fn parse_icalendar_meeting_request(lines: &[String]) -> Option<CalendarMeetingRequest> {
    let event_start = lines
        .iter()
        .position(|line| line.eq_ignore_ascii_case("BEGIN:VEVENT"))?;
    let event_end = lines
        .iter()
        .skip(event_start + 1)
        .position(|line| line.eq_ignore_ascii_case("END:VEVENT"))?
        + event_start
        + 1;
    let event = &lines[event_start + 1..event_end];
    let uid = icalendar_value(event, "UID")?;
    if uid.trim().is_empty() {
        return None;
    }

    let timezone_offsets = icalendar_timezone_offsets(lines);
    let parsed_time = |name| {
        icalendar_value_with_parameters(event, name).and_then(|(parameters, value)| {
            parse_icalendar_datetime(parameters, value, &timezone_offsets)
        })
    };
    // This bounded projection does not yet carry a recurrence pattern or an
    // arbitrary VTIMEZONE into MAPI. Do not advertise an actionable request
    // unless Outlook can receive a complete, single-instance UTC interval.
    if icalendar_value(event, "RRULE").is_some()
        || icalendar_value(event, "RECURRENCE-ID").is_some()
    {
        return None;
    }
    let meeting_start = parsed_time("DTSTART")?;
    let meeting_end = parsed_time("DTEND")?;
    if meeting_end <= meeting_start {
        return None;
    }
    let response_requested = event
        .iter()
        .filter_map(|line| icalendar_property(line, "ATTENDEE"))
        .any(|(parameters, _)| {
            icalendar_parameter(parameters, "RSVP")
                .is_some_and(|value| value.eq_ignore_ascii_case("TRUE"))
        });
    let meeting_sequence = icalendar_value(event, "X-MICROSOFT-CDO-APPT-SEQUENCE")
        .or_else(|| icalendar_value(event, "SEQUENCE"))
        .and_then(|value| value.parse::<u32>().ok())
        .and_then(|value| i32::try_from(value).ok())
        .unwrap_or(0);
    let intended_busy_status = match icalendar_value(event, "X-MICROSOFT-CDO-INTENDEDSTATUS")
        .or_else(|| icalendar_value(event, "X-MICROSOFT-CDO-BUSYSTATUS"))
        .as_deref()
        .map(str::trim)
    {
        Some(value) if value.eq_ignore_ascii_case("FREE") => 0,
        Some(value) if value.eq_ignore_ascii_case("TENTATIVE") => 1,
        Some(value) if value.eq_ignore_ascii_case("OOF") => 3,
        Some(value) if value.eq_ignore_ascii_case("WORKINGELSEWHERE") => 4,
        _ if icalendar_value(event, "TRANSP")
            .is_some_and(|value| value.eq_ignore_ascii_case("TRANSPARENT")) =>
        {
            0
        }
        _ => 2,
    };

    Some(CalendarMeetingRequest {
        uid: normalize_calendar_meeting_uid(&uid),
        transport_attachment_id: None,
        response_requested,
        sent_at: parsed_time("DTSTAMP"),
        meeting_start,
        meeting_end,
        meeting_location: icalendar_value(event, "LOCATION").filter(|value| !value.is_empty()),
        meeting_sequence,
        intended_busy_status,
    })
}

fn parse_icalendar_meeting_response(bytes: &[u8]) -> Option<CalendarMeetingResponse> {
    let lines = unfold_icalendar_lines(&String::from_utf8_lossy(bytes));
    let method = icalendar_value(&lines, "METHOD")?.to_ascii_uppercase();
    if !matches!(method.as_str(), "REPLY" | "COUNTER") {
        return None;
    }
    if lines.iter().filter(|line| *line == "BEGIN:VEVENT").count() != 1
        || lines.iter().filter(|line| *line == "END:VEVENT").count() != 1
    {
        return None;
    }
    let event_start = lines.iter().position(|line| line == "BEGIN:VEVENT")?;
    let event_end = lines
        .iter()
        .skip(event_start + 1)
        .position(|line| line == "END:VEVENT")?
        + event_start
        + 1;
    let event = &lines[event_start + 1..event_end];
    let attendees = event
        .iter()
        .filter_map(|line| icalendar_property(line, "ATTENDEE"))
        .collect::<Vec<_>>();
    let [(parameters, value)] = attendees.as_slice() else {
        return None;
    };
    let partstat = icalendar_parameter(parameters, "PARTSTAT")?.to_ascii_lowercase();
    if (method == "COUNTER" && !matches!(partstat.as_str(), "tentative" | "declined"))
        || (method == "REPLY" && !matches!(partstat.as_str(), "accepted" | "tentative" | "declined"))
    {
        return None;
    }
    let attendee_email = value
        .trim()
        .strip_prefix("mailto:")
        .or_else(|| value.trim().strip_prefix("MAILTO:"))
        .map(crate::normalize_email)
        .filter(|value| !value.is_empty())?;
    let uid = icalendar_value(event, "UID")?;
    if uid.trim().is_empty() {
        return None;
    }
    let attendee_name = icalendar_parameter(parameters, "CN")
        .unwrap_or_default()
        .trim()
        .to_string();
    let meeting_location = icalendar_value(event, "LOCATION").filter(|value| !value.is_empty());
    let meeting_sequence = icalendar_value(event, "X-MICROSOFT-CDO-APPT-SEQUENCE")
        .or_else(|| icalendar_value(event, "SEQUENCE"))
        .and_then(|value| value.parse::<u32>().ok())
        .and_then(|value| i32::try_from(value).ok());
    let timezone_offsets = icalendar_timezone_offsets(&lines);
    // [MS-OXCICAL] section 2.1.3.1.1.20.9 maps a REPLY or COUNTER DTSTAMP
    // to PidLidAttendeeCriticalChange.
    let response_sent_at = match icalendar_value_with_parameters(event, "DTSTAMP") {
        Some((parameters, value)) => {
            Some(parse_icalendar_datetime(parameters, value, &timezone_offsets)?)
        }
        None => None,
    };
    let (meeting_start, meeting_end, proposed_start, proposed_end, original_start, original_end) =
        if method == "COUNTER" {
        let start = icalendar_value_with_parameters(event, "DTSTART")?;
        let end = icalendar_value_with_parameters(event, "DTEND")?;
        // [MS-OXCICAL] sections 2.1.3.1.1.20.59-.60 define these optional
        // fields as the meeting interval for which the counter was made.
        let original_start = match icalendar_value_with_parameters(event, "X-MS-OLK-ORIGINALSTART") {
            Some((parameters, value)) => {
                Some(parse_icalendar_datetime(parameters, value, &timezone_offsets)?)
            }
            None => None,
        };
        let original_end = match icalendar_value_with_parameters(event, "X-MS-OLK-ORIGINALEND") {
            Some((parameters, value)) => {
                Some(parse_icalendar_datetime(parameters, value, &timezone_offsets)?)
            }
            None => None,
        };
        if original_start.is_some() != original_end.is_some() {
            return None;
        }
        let proposed_start = parse_icalendar_datetime(start.0, start.1, &timezone_offsets)?;
        let proposed_end = parse_icalendar_datetime(end.0, end.1, &timezone_offsets)?;
        let (meeting_start, meeting_end) = match (&original_start, &original_end) {
            (Some(start), Some(end)) => (start.clone(), end.clone()),
            _ => (proposed_start.clone(), proposed_end.clone()),
        };
        (
            Some(meeting_start),
            Some(meeting_end),
            Some(proposed_start),
            Some(proposed_end),
            original_start,
            original_end,
        )
    } else {
        match (
            icalendar_value_with_parameters(event, "DTSTART"),
            icalendar_value_with_parameters(event, "DTEND"),
        ) {
            (None, None) => (None, None, None, None, None, None),
            (Some(start), Some(end)) => (
                Some(parse_icalendar_datetime(start.0, start.1, &timezone_offsets)?),
                Some(parse_icalendar_datetime(end.0, end.1, &timezone_offsets)?),
                None,
                None,
                None,
                None,
            ),
            _ => return None,
        }
    };
    Some(CalendarMeetingResponse {
        method,
        attendee_email,
        attendee_name,
        partstat,
        uid: normalize_calendar_meeting_uid(&uid),
        response_sent_at,
        meeting_start,
        meeting_end,
        meeting_location,
        meeting_sequence,
        proposed_start,
        proposed_end,
        original_start,
        original_end,
    })
}

fn unfold_icalendar_lines(value: &str) -> Vec<String> {
    let mut lines: Vec<String> = Vec::new();
    for line in value.lines().map(|line| line.trim_end_matches('\r')) {
        if (line.starts_with(' ') || line.starts_with('\t')) && !lines.is_empty() {
            lines.last_mut().expect("iCalendar line exists").push_str(line.trim_start());
        } else {
            lines.push(line.to_string());
        }
    }
    lines
}

fn icalendar_property<'a>(line: &'a str, name: &str) -> Option<(&'a str, &'a str)> {
    let (left, value) = line.split_once(':')?;
    let (property_name, parameters) = left.split_once(';').unwrap_or((left, ""));
    property_name.eq_ignore_ascii_case(name).then_some((parameters, value))
}

fn icalendar_value(lines: &[String], name: &str) -> Option<String> {
    lines.iter().find_map(|line| {
        icalendar_property(line, name).map(|(_, value)| value.trim().to_string())
    })
}

fn icalendar_value_with_parameters<'a>(
    lines: &'a [String],
    name: &str,
) -> Option<(&'a str, &'a str)> {
    lines.iter().find_map(|line| icalendar_property(line, name))
}

fn icalendar_parameter(parameters: &str, name: &str) -> Option<String> {
    parameters.split(';').find_map(|parameter| {
        let (key, value) = parameter.split_once('=')?;
        key.eq_ignore_ascii_case(name)
            .then_some(value.trim_matches('"').to_string())
    })
}

fn icalendar_timezone_offsets(lines: &[String]) -> HashMap<String, i32> {
    let mut offsets = HashMap::<String, Vec<i32>>::new();
    let mut current_tzid = None;
    for line in lines {
        if line == "BEGIN:VTIMEZONE" {
            current_tzid = None;
        } else if let Some((_, value)) = icalendar_property(line, "TZID") {
            current_tzid = Some(value.trim().to_string());
        } else if let Some((_, value)) = icalendar_property(line, "TZOFFSETTO") {
            if let (Some(tzid), Some(offset)) = (current_tzid.as_ref(), parse_icalendar_offset(value)) {
                offsets.entry(tzid.clone()).or_default().push(offset);
            }
        }
    }
    offsets
        .into_iter()
        .filter_map(|(tzid, values)| {
            values
                .first()
                .copied()
                .filter(|first| values.iter().all(|value| value == first))
                .map(|offset| (tzid, offset))
        })
        .collect()
}

fn parse_icalendar_offset(value: &str) -> Option<i32> {
    let bytes = value.trim().as_bytes();
    if bytes.len() != 5
        || !matches!(bytes[0], b'+' | b'-')
        || !bytes[1..].iter().all(u8::is_ascii_digit)
    {
        return None;
    }
    let hours = i32::from((bytes[1] - b'0') * 10 + bytes[2] - b'0');
    let minutes = i32::from((bytes[3] - b'0') * 10 + bytes[4] - b'0');
    (hours <= 23 && minutes <= 59).then_some(
        (hours * 3_600 + minutes * 60) * if bytes[0] == b'-' { -1 } else { 1 },
    )
}

fn parse_icalendar_datetime(
    parameters: &str,
    value: &str,
    timezone_offsets: &HashMap<String, i32>,
) -> Option<String> {
    let value = value.trim();
    let utc = value.ends_with('Z');
    let value = value.strip_suffix('Z').unwrap_or(value);
    if value.len() != 15
        || value.as_bytes().get(8) != Some(&b'T')
        || !value
            .as_bytes()
            .iter()
            .enumerate()
            .all(|(index, byte)| index == 8 || byte.is_ascii_digit())
    {
        return None;
    }
    let parse = |start, end| value.get(start..end)?.parse::<u32>().ok();
    let date = NaiveDate::from_ymd_opt(parse(0, 4)? as i32, parse(4, 6)?, parse(6, 8)?)?;
    let date_time = date.and_hms_opt(parse(9, 11)?, parse(11, 13)?, parse(13, 15)?)?;
    let offset = if utc {
        0
    } else {
        let tzid = icalendar_parameter(parameters, "TZID")?;
        *timezone_offsets.get(&tzid)?
    };
    Some(
        FixedOffset::east_opt(offset)?
            .from_local_datetime(&date_time)
            .single()?
            .with_timezone(&Utc)
            .format("%Y-%m-%dT%H:%M:%SZ")
            .to_string(),
    )
}

pub fn parse_message_attachments(bytes: &[u8]) -> Result<Vec<AttachmentUploadInput>> {
    collect_mime_attachment_parts(bytes)?
        .into_iter()
        .enumerate()
        .map(|(index, mut attachment)| {
            let is_calendar = attachment.declared_mime.as_deref().is_some_and(|value| {
                value
                    .trim()
                    .to_ascii_lowercase()
                    .starts_with("text/calendar")
            });
            let file_name = attachment.filename.unwrap_or_else(|| {
                if is_calendar {
                    format!("calendar-{}.ics", index + 1)
                } else {
                    format!("attachment-{}.bin", index + 1)
                }
            });
            let media_type = attachment
                .declared_mime
                .unwrap_or_else(|| "application/octet-stream".to_string());
            trim_single_structural_crlf(&mut attachment.bytes);
            Ok(AttachmentUploadInput {
                file_name,
                media_type,
                disposition: attachment.content_disposition.as_deref().and_then(|value| {
                    let disposition = value.split(';').next().map(str::trim)?;
                    if disposition.eq_ignore_ascii_case("inline") {
                        Some("inline".to_string())
                    } else if disposition.eq_ignore_ascii_case("attachment") {
                        Some("attachment".to_string())
                    } else {
                        None
                    }
                }),
                content_id: attachment.content_id,
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
    let (header_text, _) = raw.split_once("\n\n").unwrap_or((raw.as_str(), ""));
    let headers = parse_headers(header_text);
    let visible = extract_visible_body_parts(bytes)?;

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
        body_text: visible.text_body,
        body_html_sanitized: visible.html_body,
        attachments: parse_message_attachments(bytes)?,
    })
}

pub fn parse_headers_map(raw_message: &[u8]) -> HashMap<String, String> {
    let raw = String::from_utf8_lossy(raw_message).replace("\r\n", "\n");
    let (header_text, _) = raw.split_once("\n\n").unwrap_or((raw.as_str(), ""));
    parse_headers(header_text)
}

pub fn parse_header_records(raw_message: &[u8]) -> Vec<ParsedRfc822Header> {
    let mut headers = Vec::new();
    let mut current_name: Option<String> = None;
    let mut current_value = String::new();

    for line in String::from_utf8_lossy(raw_message).lines() {
        let line = line.trim_end_matches('\r');
        if line.is_empty() {
            break;
        }

        if line.starts_with(' ') || line.starts_with('\t') {
            if !current_value.is_empty() {
                current_value.push(' ');
            }
            current_value.push_str(line.trim());
            continue;
        }

        if let Some(name) = current_name.take() {
            headers.push(ParsedRfc822Header {
                name,
                value: current_value.trim().to_string(),
            });
            current_value.clear();
        }

        if let Some((name, value)) = line.split_once(':') {
            current_name = Some(name.trim().to_string());
            current_value.push_str(value.trim());
        }
    }

    if let Some(name) = current_name {
        headers.push(ParsedRfc822Header {
            name,
            value: current_value.trim().to_string(),
        });
    }

    headers
}

pub fn parse_message_date_header(raw_message: &[u8]) -> Option<String> {
    parse_header_records(raw_message)
        .into_iter()
        .find(|header| header.name.eq_ignore_ascii_case("date"))
        .and_then(|header| parse_mail_datetime(&header.value))
}

fn parse_mail_datetime(value: &str) -> Option<String> {
    DateTime::parse_from_rfc2822(value)
        .or_else(|_| DateTime::parse_from_rfc3339(value))
        .ok()
        .map(|value| {
            value
                .with_timezone(&Utc)
                .format("%Y-%m-%dT%H:%M:%SZ")
                .to_string()
        })
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
        let email = crate::normalize_email(address.trim().trim_end_matches('>'));
        if email.is_empty() {
            return None;
        }
        let display_name = display.trim().trim_matches('"').trim().to_string();
        return Some(ParsedMailAddress {
            email,
            display_name: (!display_name.is_empty()).then_some(display_name),
        });
    }

    let email = crate::normalize_email(trimmed.trim_matches(['<', '>']).trim_matches('"'));
    if email.is_empty() {
        None
    } else {
        Some(ParsedMailAddress {
            email,
            display_name: None,
        })
    }
}

fn trim_single_structural_crlf(bytes: &mut Vec<u8>) {
    if bytes.ends_with(b"\r\n") {
        bytes.truncate(bytes.len() - 2);
    } else if bytes.ends_with(b"\n") || bytes.ends_with(b"\r") {
        bytes.truncate(bytes.len() - 1);
    }
}

#[cfg(test)]
mod tests {
    use super::{
        parse_calendar_meeting_response, parse_header_recipients, parse_message_attachments,
        parse_message_date_header, parse_rfc822_message,
    };

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
    fn parse_message_date_header_normalizes_rfc2822_date_to_utc() {
        let raw = concat!(
            "From: Sender <sender@example.test>\r\n",
            "Date: Tue, 9 Jun 2026 19:25:15 +0000\r\n",
            "Subject: Date\r\n",
            "\r\n",
            "Body\r\n"
        );

        assert_eq!(
            parse_message_date_header(raw.as_bytes()).as_deref(),
            Some("2026-06-09T19:25:15Z")
        );
    }

    #[test]
    fn parse_message_date_header_unfolds_before_parsing() {
        let raw = concat!(
            "From: Sender <sender@example.test>\r\n",
            "Date: Tue, 9 Jun 2026\r\n",
            "  21:25:15 +0200\r\n",
            "Subject: Date\r\n",
            "\r\n",
            "Body\r\n"
        );

        assert_eq!(
            parse_message_date_header(raw.as_bytes()).as_deref(),
            Some("2026-06-09T19:25:15Z")
        );
    }

    #[test]
    fn parse_message_attachments_trims_structural_boundary_crlf() {
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
        assert_eq!(attachments[0].blob_bytes, b"PDFDATA".to_vec());
    }

    #[test]
    fn parse_message_attachments_preserves_calendar_request_media_type() {
        let message = concat!(
            "Content-Type: multipart/mixed; boundary=\"invite\"\r\n",
            "\r\n",
            "--invite\r\n",
            "Content-Type: text/calendar; method=REQUEST; charset=UTF-8\r\n",
            "Content-Disposition: inline; filename=\"invite.ics\"\r\n",
            "\r\n",
            "BEGIN:VCALENDAR\r\n",
            "METHOD:REQUEST\r\n",
            "BEGIN:VEVENT\r\n",
            "UID:probe-7@\r\n",
            " example.test\r\n",
            "DTSTAMP:20260823T180000Z\r\n",
            "DTSTART:20260824T063000Z\r\n",
            "DTEND:20260824T070000Z\r\n",
            "LOCATION:Les Planches\r\n",
            "SEQUENCE:2\r\n",
            "X-MICROSOFT-CDO-BUSYSTATUS:FREE\r\n",
            "X-MICROSOFT-CDO-INTENDEDSTATUS:OOF\r\n",
            "ATTENDEE;RSVP=FALSE:mailto:observer@example.test\r\n",
            "ATTENDEE;RSVP=TRUE:mailto:test@l-p-e.ch\r\n",
            "END:VEVENT\r\n",
            "END:VCALENDAR\r\n",
            "--invite--\r\n"
        );

        let attachments = parse_message_attachments(message.as_bytes()).unwrap();

        assert_eq!(attachments.len(), 1);
        assert_eq!(attachments[0].file_name, "invite.ics");
        assert_eq!(
            attachments[0].media_type,
            "text/calendar; method=REQUEST; charset=UTF-8"
        );
        assert_eq!(
            super::parse_calendar_meeting_request(&attachments),
            Some(super::CalendarMeetingRequest {
                uid: "probe-7@example.test".to_string(),
                transport_attachment_id: None,
                response_requested: true,
                sent_at: Some("2026-08-23T18:00:00Z".to_string()),
                meeting_start: "2026-08-24T06:30:00Z".to_string(),
                meeting_end: "2026-08-24T07:00:00Z".to_string(),
                meeting_location: Some("Les Planches".to_string()),
                meeting_sequence: 2,
                intended_busy_status: 3,
            })
        );
    }

    #[test]
    fn calendar_meeting_request_requires_matching_body_method_and_one_event() {
        let attachment = |media_type: &str, body: &str| crate::AttachmentUploadInput {
            file_name: "invite.ics".to_string(),
            media_type: media_type.to_string(),
            disposition: Some("inline".to_string()),
            content_id: None,
            blob_bytes: body.as_bytes().to_vec(),
        };
        assert_eq!(
            super::parse_calendar_meeting_request(&[attachment(
                "text/calendar; method=REQUEST",
                "BEGIN:VCALENDAR\r\nMETHOD:PUBLISH\r\nBEGIN:VEVENT\r\nUID:x\r\nDTSTART:20260824T063000Z\r\nDTEND:20260824T070000Z\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
            )]),
            None
        );
        assert_eq!(
            super::parse_calendar_meeting_request(&[attachment(
                "text/calendar; method=PUBLISH",
                "BEGIN:VCALENDAR\r\nMETHOD:REQUEST\r\nBEGIN:VEVENT\r\nUID:x\r\nDTSTART:20260824T063000Z\r\nDTEND:20260824T070000Z\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
            )]),
            None
        );
        assert_eq!(
            super::parse_calendar_meeting_request(&[attachment(
                "text/calendar; method=REQUEST",
                "BEGIN:VCALENDAR\r\nMETHOD:REQUEST\r\nBEGIN:VEVENT\r\nUID:x\r\nDTSTART:20260824T063000Z\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
            )]),
            None
        );
        assert_eq!(
            super::parse_calendar_meeting_request(&[attachment(
                "text/calendar; method=REQUEST",
                "BEGIN:VCALENDAR\r\nMETHOD:REQUEST\r\nBEGIN:VEVENT\r\nUID:x\r\nDTSTART:20260824T063000Z\r\nDTEND:20260824T070000Z\r\nRRULE:FREQ=DAILY;COUNT=2\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
            )]),
            None
        );
        assert_eq!(
            super::parse_calendar_meeting_request(&[attachment(
                "text/calendar; method=REQUEST",
                "BEGIN:VCALENDAR\r\nMETHOD:REQUEST\r\nBEGIN:VEVENT\r\nUID:x\r\nRECURRENCE-ID:20260825T063000Z\r\nDTSTART:20260825T063000Z\r\nDTEND:20260825T070000Z\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
            )]),
            None
        );
    }

    #[test]
    fn parse_message_attachments_names_unnamed_inline_calendar_parts() {
        let message = concat!(
            "Content-Type: multipart/alternative; boundary=\"invite\"\r\n",
            "\r\n",
            "--invite\r\n",
            "Content-Type: text/plain; charset=utf-8\r\n",
            "\r\n",
            "\r\n",
            "--invite\r\n",
            "Content-Type: text/calendar; charset=utf-8; method=COUNTER\r\n",
            "Content-Transfer-Encoding: base64\r\n",
            "\r\n",
            "QkVHSU46VkNBTEVOREFSDQpNRVRIT0Q6Q09VTlRFUg0KRU5EOlZDQUxFTkRBUg==\r\n",
            "--invite--\r\n"
        );

        let attachments = parse_message_attachments(message.as_bytes()).unwrap();

        assert_eq!(attachments.len(), 1);
        assert_eq!(attachments[0].file_name, "calendar-1.ics");
        assert_eq!(attachments[0].disposition.as_deref(), Some("inline"));
        assert_eq!(
            attachments[0].media_type,
            "text/calendar; charset=utf-8; method=COUNTER"
        );
        assert_eq!(
            attachments[0].blob_bytes,
            b"BEGIN:VCALENDAR\r\nMETHOD:COUNTER\r\nEND:VCALENDAR".to_vec()
        );
    }

    #[test]
    fn parse_calendar_meeting_response_accepts_outlook_counter_with_fixed_offset_timezone() {
        let attachments = parse_message_attachments(
            concat!(
                "Content-Type: text/calendar; method=COUNTER; charset=UTF-8\r\n",
                "\r\n",
                "BEGIN:VCALENDAR\r\n",
                "METHOD:COUNTER\r\n",
                "BEGIN:VTIMEZONE\r\n",
                "TZID:Greenwich Standard Time\r\n",
                "BEGIN:STANDARD\r\n",
                "TZOFFSETTO:+0000\r\n",
                "END:STANDARD\r\n",
                "END:VTIMEZONE\r\n",
                "BEGIN:VEVENT\r\n",
                "ATTENDEE;PARTSTAT=TENTATIVE;CN=Denis Ducret:mailto:denis.ducret@sdic.ch\r\n",
                "DTSTART;TZID=Greenwich Standard Time:20260824T063000\r\n",
                "DTEND;TZID=Greenwich Standard Time:20260824T073000\r\n",
                "DTSTAMP:20260821T170000Z\r\n",
                "UID:mapi-goid:001122\r\n",
                "END:VEVENT\r\n",
                "END:VCALENDAR\r\n"
            )
            .as_bytes(),
        )
        .unwrap();

        assert_eq!(
            parse_calendar_meeting_response(&attachments),
            Some(super::CalendarMeetingResponse {
                method: "COUNTER".to_string(),
                attendee_email: "denis.ducret@sdic.ch".to_string(),
                attendee_name: "Denis Ducret".to_string(),
                partstat: "tentative".to_string(),
                uid: "mapi-goid:001122".to_string(),
                response_sent_at: Some("2026-08-21T17:00:00Z".to_string()),
                meeting_start: Some("2026-08-24T06:30:00Z".to_string()),
                meeting_end: Some("2026-08-24T07:30:00Z".to_string()),
                meeting_location: None,
                meeting_sequence: None,
                proposed_start: Some("2026-08-24T06:30:00Z".to_string()),
                proposed_end: Some("2026-08-24T07:30:00Z".to_string()),
                original_start: None,
                original_end: None,
            })
        );
    }

    #[test]
    fn parse_calendar_meeting_response_accepts_outlook_declined_counter() {
        let attachments = parse_message_attachments(
            concat!(
                "Content-Type: text/calendar; method=COUNTER; charset=UTF-8\r\n",
                "\r\n",
                "BEGIN:VCALENDAR\r\n",
                "METHOD:COUNTER\r\n",
                "VERSION:2.0\r\n",
                "BEGIN:VTIMEZONE\r\n",
                "TZID:UTC\r\n",
                "BEGIN:STANDARD\r\n",
                "TZOFFSETTO:+0000\r\n",
                "END:STANDARD\r\n",
                "BEGIN:DAYLIGHT\r\n",
                "TZOFFSETTO:+0000\r\n",
                "END:DAYLIGHT\r\n",
                "END:VTIMEZONE\r\n",
                "BEGIN:VEVENT\r\n",
                "ATTENDEE;PARTSTAT=DECLINED;CN=Denis Ducret:mailto:denis.ducret@sdic.ch\r\n",
                "DTSTART;TZID=UTC:20260824T123000\r\n",
                "DTEND;TZID=UTC:20260824T130000\r\n",
                "X-MS-OLK-ORIGINALSTART;TZID=UTC:20260824T090000\r\n",
                "X-MS-OLK-ORIGINALEND;TZID=UTC:20260824T093000\r\n",
                "LOCATION;LANGUAGE=en-US:Les Planches\r\n",
                "UID:mapi-goid:040000008200e00074c5b7101a82e00800000000c08470cd9e31dd0100000\r\n",
                " 0000000000010000000ecff8aec00ce584390f914bf6a87f955\r\n",
                "SEQUENCE:0\r\n",
                "END:VEVENT\r\n",
                "END:VCALENDAR\r\n"
            )
            .as_bytes(),
        )
        .unwrap();

        assert_eq!(
            parse_calendar_meeting_response(&attachments),
            Some(super::CalendarMeetingResponse {
                method: "COUNTER".to_string(),
                attendee_email: "denis.ducret@sdic.ch".to_string(),
                attendee_name: "Denis Ducret".to_string(),
                partstat: "declined".to_string(),
                uid: "mapi-goid:040000008200e00074c5b7101a82e00800000000c08470cd9e31dd01000000000000000010000000ecff8aec00ce584390f914bf6a87f955".to_string(),
                response_sent_at: None,
                meeting_start: Some("2026-08-24T09:00:00Z".to_string()),
                meeting_end: Some("2026-08-24T09:30:00Z".to_string()),
                meeting_location: Some("Les Planches".to_string()),
                meeting_sequence: Some(0),
                proposed_start: Some("2026-08-24T12:30:00Z".to_string()),
                proposed_end: Some("2026-08-24T13:00:00Z".to_string()),
                original_start: Some("2026-08-24T09:00:00Z".to_string()),
                original_end: Some("2026-08-24T09:30:00Z".to_string()),
            })
        );
    }

    #[test]
    fn parse_calendar_meeting_response_rejects_ambiguous_counter_payloads() {
        let multiple_events = parse_message_attachments(
            concat!(
                "Content-Type: text/calendar; method=COUNTER; charset=UTF-8\r\n",
                "\r\n",
                "BEGIN:VCALENDAR\r\n",
                "METHOD:COUNTER\r\n",
                "BEGIN:VEVENT\r\n",
                "ATTENDEE;PARTSTAT=TENTATIVE:mailto:denis.ducret@sdic.ch\r\n",
                "DTSTART:20260824T063000Z\r\n",
                "DTEND:20260824T070000Z\r\n",
                "UID:mapi-goid:001122\r\n",
                "END:VEVENT\r\n",
                "BEGIN:VEVENT\r\n",
                "UID:mapi-goid:334455\r\n",
                "END:VEVENT\r\n",
                "END:VCALENDAR\r\n"
            )
            .as_bytes(),
        )
        .unwrap();
        assert!(parse_calendar_meeting_response(&multiple_events).is_none());

        let partial_original = parse_message_attachments(
            concat!(
                "Content-Type: text/calendar; method=COUNTER; charset=UTF-8\r\n",
                "\r\n",
                "BEGIN:VCALENDAR\r\n",
                "METHOD:COUNTER\r\n",
                "BEGIN:VEVENT\r\n",
                "ATTENDEE;PARTSTAT=TENTATIVE:mailto:denis.ducret@sdic.ch\r\n",
                "DTSTART:20260824T063000Z\r\n",
                "DTEND:20260824T070000Z\r\n",
                "X-MS-OLK-ORIGINALSTART:20260824T060000Z\r\n",
                "UID:mapi-goid:001122\r\n",
                "END:VEVENT\r\n",
                "END:VCALENDAR\r\n"
            )
            .as_bytes(),
        )
        .unwrap();
        assert!(parse_calendar_meeting_response(&partial_original).is_none());
    }

    #[test]
    fn normalizes_mapi_global_object_id_calendar_uids() {
        assert_eq!(
            super::normalize_calendar_meeting_uid(" MAPI-GOID:0011aAbb "),
            "mapi-goid:0011aabb"
        );
        assert_eq!(
            super::normalize_calendar_meeting_uid("opaque-uid@example.test"),
            "opaque-uid@example.test"
        );
    }

    #[test]
    fn parse_calendar_meeting_response_accepts_reply_and_clears_proposal_fields() {
        let attachments = parse_message_attachments(
            concat!(
                "Content-Type: text/calendar; method=REPLY; charset=UTF-8\r\n",
                "\r\n",
                "BEGIN:VCALENDAR\r\n",
                "METHOD:REPLY\r\n",
                "BEGIN:VEVENT\r\n",
                "ATTENDEE;PARTSTAT=ACCEPTED:mailto:denis.ducret@sdic.ch\r\n",
                "DTSTART:20260825T080000Z\r\n",
                "DTEND:20260825T083000Z\r\n",
                "UID:MAPI-GOID:001122\r\n",
                "END:VEVENT\r\n",
                "END:VCALENDAR\r\n"
            )
            .as_bytes(),
        )
        .unwrap();

        assert_eq!(
            parse_calendar_meeting_response(&attachments),
            Some(super::CalendarMeetingResponse {
                method: "REPLY".to_string(),
                attendee_email: "denis.ducret@sdic.ch".to_string(),
                attendee_name: String::new(),
                partstat: "accepted".to_string(),
                uid: "mapi-goid:001122".to_string(),
                response_sent_at: None,
                meeting_start: Some("2026-08-25T08:00:00Z".to_string()),
                meeting_end: Some("2026-08-25T08:30:00Z".to_string()),
                meeting_location: None,
                meeting_sequence: None,
                proposed_start: None,
                proposed_end: None,
                original_start: None,
                original_end: None,
            })
        );
    }

    #[test]
    fn parse_message_attachments_preserves_inline_content_id_metadata() {
        let message = concat!(
            "Content-Type: multipart/related; boundary=\"rel\"\r\n",
            "\r\n",
            "--rel\r\n",
            "Content-Type: text/html\r\n",
            "\r\n",
            "<img src=\"cid:logo@example.test\">\r\n",
            "--rel\r\n",
            "Content-Type: image/png; name=\"logo.png\"\r\n",
            "Content-Disposition: inline; filename=\"logo.png\"\r\n",
            "Content-ID: <logo@example.test>\r\n",
            "\r\n",
            "PNGDATA\r\n",
            "--rel--\r\n"
        );

        let attachments = parse_message_attachments(message.as_bytes()).unwrap();

        assert_eq!(attachments.len(), 1);
        assert_eq!(attachments[0].disposition.as_deref(), Some("inline"));
        assert_eq!(
            attachments[0].content_id.as_deref(),
            Some("logo@example.test")
        );
        assert_eq!(attachments[0].file_name, "logo.png");
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
        assert_eq!(parsed.body_text, "Hello");
        assert_eq!(parsed.attachments.len(), 1);
        assert_eq!(parsed.attachments[0].file_name, "notes.odt");
        assert_eq!(
            parsed.attachments[0].media_type,
            "application/vnd.oasis.opendocument.text"
        );
        assert_eq!(parsed.attachments[0].blob_bytes, b"ODT-DATA".to_vec());
    }

    #[test]
    fn parse_rfc822_message_prefers_plaintext_but_keeps_html_body() {
        let message = concat!(
            "Subject: Multipart\r\n",
            "Content-Type: multipart/alternative; boundary=\"b1\"\r\n",
            "\r\n",
            "--b1\r\n",
            "Content-Type: text/plain; charset=utf-8\r\n",
            "\r\n",
            "Plain body\r\n",
            "--b1\r\n",
            "Content-Type: text/html; charset=utf-8\r\n",
            "\r\n",
            "<p>HTML body</p>\r\n",
            "--b1--\r\n"
        );

        let parsed = parse_rfc822_message(message.as_bytes()).unwrap();

        assert_eq!(parsed.body_text, "Plain body");
        assert_eq!(
            parsed.body_html_sanitized.as_deref(),
            Some("<p>HTML body</p>")
        );
    }
}
