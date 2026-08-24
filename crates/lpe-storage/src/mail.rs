use anyhow::Result;
use lpe_magika::{collect_mime_attachment_parts, extract_visible_body_parts};
use serde::{Deserialize, Serialize};
use sqlx::types::chrono::{DateTime, Utc};
use std::collections::HashMap;
use uuid::Uuid;

use crate::{AttachmentUploadInput, SubmittedRecipientInput};

mod global_object_id;
mod icalendar_timezone;

use global_object_id::calendar_uid_has_occurrence_date;
pub use global_object_id::{
    calendar_uid_from_global_object_id, decode_calendar_global_object_id_uid,
    external_calendar_uid, normalize_calendar_meeting_uid,
};
use icalendar_timezone::{icalendar_timezones, parse_icalendar_datetime};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedMailAddress {
    pub email: String,
    pub display_name: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ParsedRfc822Message {
    pub from: Option<ParsedMailAddress>,
    pub from_is_unambiguous: bool,
    pub sender: Option<ParsedMailAddress>,
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CalendarMeetingIdentity {
    pub email: String,
    pub display_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CalendarMeetingAttendee {
    pub email: String,
    pub display_name: String,
    pub cutype: String,
    pub role: String,
    pub partstat: String,
    pub rsvp: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CalendarMeetingResponse {
    pub method: String,
    #[serde(skip, default)]
    pub transport_attachment_id: Option<Uuid>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub server_processed: bool,
    pub organizer: Option<CalendarMeetingIdentity>,
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CalendarMeetingRequest {
    pub uid: String,
    #[serde(skip, default)]
    pub transport_attachment_id: Option<Uuid>,
    pub organizer: Option<CalendarMeetingIdentity>,
    pub attendees: Vec<CalendarMeetingAttendee>,
    pub response_requested: bool,
    pub sent_at: Option<String>,
    pub meeting_start: String,
    pub meeting_end: String,
    pub meeting_location: Option<String>,
    pub meeting_sequence: i32,
    pub intended_busy_status: i32,
}

pub fn parse_calendar_meeting_response(
    attachments: &[AttachmentUploadInput],
) -> Option<CalendarMeetingResponse> {
    parse_calendar_meeting_response_with_content_sha256(attachments).map(|(response, _)| response)
}

pub(crate) fn is_text_calendar_media_type(media_type: &str) -> bool {
    media_type
        .split(';')
        .next()
        .is_some_and(|value| value.trim().eq_ignore_ascii_case("text/calendar"))
}

pub(crate) fn parse_calendar_meeting_response_with_content_sha256(
    attachments: &[AttachmentUploadInput],
) -> Option<(CalendarMeetingResponse, String)> {
    attachments.iter().find_map(|attachment| {
        (attachment.is_scheduling_body && is_text_calendar_media_type(&attachment.media_type))
            .then(|| {
                let response = parse_icalendar_meeting_response(&attachment.blob_bytes)?;
                if declared_calendar_method(&attachment.media_type)
                    .is_some_and(|declared| declared != response.method)
                {
                    return None;
                }
                Some((response, crate::sha256_hex(&attachment.blob_bytes)))
            })
            .flatten()
    })
}

pub fn parse_calendar_meeting_request(
    attachments: &[AttachmentUploadInput],
) -> Option<CalendarMeetingRequest> {
    // [MS-OXCMAIL] section 2.2.3.3.2 and [MS-OXCICAL] section 2.1.3.1.1.1:
    // the decoded text/calendar METHOD, not a legacy outer Content-Class, owns classification.
    attachments.iter().find_map(|attachment| {
        if !attachment.is_scheduling_body || !is_text_calendar_media_type(&attachment.media_type) {
            return None;
        }
        let declared_method = declared_calendar_method(&attachment.media_type);
        if declared_method
            .as_deref()
            .is_some_and(|method| method != "REQUEST")
        {
            return None;
        }
        let lines = unfold_icalendar_lines(&String::from_utf8_lossy(&attachment.blob_bytes));
        if !single_icalendar_property(&lines, "METHOD")
            .is_some_and(|(_, method)| method.trim().eq_ignore_ascii_case("REQUEST"))
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

fn declared_calendar_method(media_type: &str) -> Option<String> {
    media_type.split(';').skip(1).find_map(|parameter| {
        let (name, value) = parameter.split_once('=')?;
        name.trim()
            .eq_ignore_ascii_case("method")
            .then(|| value.trim().trim_matches('"').to_ascii_uppercase())
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
    let (_, uid) = single_icalendar_property(event, "UID")?;
    let uid = parse_icalendar_uid(uid)?;

    let timezones = icalendar_timezones(lines);
    let parsed_required_time = |name| {
        single_icalendar_property(event, name)
            .and_then(|(parameters, value)| parse_icalendar_datetime(parameters, value, &timezones))
    };
    // This bounded projection does not yet carry an event recurrence pattern
    // into MAPI. Do not advertise an actionable request unless Outlook can
    // receive a complete, single-instance interval with a resolvable zone.
    if icalendar_value(event, "RRULE").is_some()
        || icalendar_value(event, "RECURRENCE-ID").is_some()
    {
        return None;
    }
    let meeting_start = parsed_required_time("DTSTART")?;
    let meeting_end = parsed_required_time("DTEND")?;
    if meeting_end <= meeting_start {
        return None;
    }
    let organizer = optional_single_icalendar_identity(event, "ORGANIZER")?;
    let attendees = icalendar_attendees(event);
    let response_requested = attendees.iter().any(|attendee| attendee.rsvp);
    let meeting_sequence = optional_icalendar_sequence(event)?.unwrap_or(0);
    let intended_status =
        optional_single_icalendar_property(event, "X-MICROSOFT-CDO-INTENDEDSTATUS")?;
    let busy_status = optional_single_icalendar_property(event, "X-MICROSOFT-CDO-BUSYSTATUS")?;
    let transparency = optional_single_icalendar_property(event, "TRANSP")?;
    let intended_busy_status = match intended_status
        .or(busy_status)
        .map(|(_, value)| value.trim())
    {
        Some(value) if value.eq_ignore_ascii_case("FREE") => 0,
        Some(value) if value.eq_ignore_ascii_case("TENTATIVE") => 1,
        Some(value) if value.eq_ignore_ascii_case("OOF") => 3,
        Some(value) if value.eq_ignore_ascii_case("WORKINGELSEWHERE") => 4,
        _ if transparency
            .is_some_and(|(_, value)| value.trim().eq_ignore_ascii_case("TRANSPARENT")) =>
        {
            0
        }
        _ => 2,
    };

    Some(CalendarMeetingRequest {
        uid: normalize_calendar_meeting_uid(&uid),
        transport_attachment_id: None,
        organizer,
        attendees,
        response_requested,
        sent_at: match optional_single_icalendar_property(event, "DTSTAMP")? {
            Some((parameters, value)) => {
                Some(parse_icalendar_datetime(parameters, value, &timezones)?)
            }
            None => None,
        },
        meeting_start,
        meeting_end,
        meeting_location: optional_single_icalendar_property(event, "LOCATION")?
            .map(|(_, value)| value.trim().to_string())
            .filter(|value| !value.is_empty()),
        meeting_sequence,
        intended_busy_status,
    })
}

fn parse_icalendar_meeting_response(bytes: &[u8]) -> Option<CalendarMeetingResponse> {
    let lines = unfold_icalendar_lines(&String::from_utf8_lossy(bytes));
    let (_, method) = single_icalendar_property(&lines, "METHOD")?;
    let method = method.trim().to_ascii_uppercase();
    if !matches!(method.as_str(), "REPLY" | "COUNTER") {
        return None;
    }
    if lines
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
    // Occurrence responses need RECURRENCE-ID-to-exception correlation. Until
    // that canonical path exists, do not apply them to a series master.
    if optional_single_icalendar_property(event, "RECURRENCE-ID")?.is_some() {
        return None;
    }
    let attendees = event
        .iter()
        .filter_map(|line| icalendar_property(line, "ATTENDEE"))
        .collect::<Vec<_>>();
    let [(parameters, value)] = attendees.as_slice() else {
        return None;
    };
    let partstat = icalendar_parameter(parameters, "PARTSTAT")?.to_ascii_lowercase();
    if (method == "COUNTER" && !matches!(partstat.as_str(), "tentative" | "declined"))
        || (method == "REPLY"
            && !matches!(partstat.as_str(), "accepted" | "tentative" | "declined"))
    {
        return None;
    }
    let attendee_email = icalendar_mailto_address(value)?;
    let (_, uid) = single_icalendar_property(event, "UID")?;
    let uid = parse_icalendar_uid(uid)?;
    let attendee_name = icalendar_parameter(parameters, "CN")
        .unwrap_or_default()
        .trim()
        .to_string();
    let meeting_location = optional_single_icalendar_property(event, "LOCATION")?
        .map(|(_, value)| value.trim().to_string())
        .filter(|value| !value.is_empty());
    let meeting_sequence = optional_icalendar_sequence(event)?;
    let timezones = icalendar_timezones(&lines);
    // [MS-OXCICAL] section 2.1.3.1.1.20.9 maps a REPLY or COUNTER DTSTAMP
    // to PidLidAttendeeCriticalChange.
    let response_sent_at = match optional_single_icalendar_property(event, "DTSTAMP")? {
        Some((parameters, value)) => Some(parse_icalendar_datetime(parameters, value, &timezones)?),
        None => None,
    };
    let (meeting_start, meeting_end, proposed_start, proposed_end, original_start, original_end) =
        if method == "COUNTER" {
            let start = single_icalendar_property(event, "DTSTART")?;
            let end = single_icalendar_property(event, "DTEND")?;
            // [MS-OXCICAL] sections 2.1.3.1.1.20.59-.60 define these optional
            // fields as the meeting interval for which the counter was made.
            let original_start =
                match optional_single_icalendar_property(event, "X-MS-OLK-ORIGINALSTART")? {
                    Some((parameters, value)) => {
                        Some(parse_icalendar_datetime(parameters, value, &timezones)?)
                    }
                    None => None,
                };
            let original_end =
                match optional_single_icalendar_property(event, "X-MS-OLK-ORIGINALEND")? {
                    Some((parameters, value)) => {
                        Some(parse_icalendar_datetime(parameters, value, &timezones)?)
                    }
                    None => None,
                };
            if original_start.is_some() != original_end.is_some() {
                return None;
            }
            let proposed_start = parse_icalendar_datetime(start.0, start.1, &timezones)?;
            let proposed_end = parse_icalendar_datetime(end.0, end.1, &timezones)?;
            if proposed_end <= proposed_start {
                return None;
            }
            if matches!(
                (&original_start, &original_end),
                (Some(start), Some(end)) if end <= start
            ) {
                return None;
            }
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
                optional_single_icalendar_property(event, "DTSTART")?,
                optional_single_icalendar_property(event, "DTEND")?,
            ) {
                (None, None) => (None, None, None, None, None, None),
                (Some(start), Some(end)) => {
                    let start = parse_icalendar_datetime(start.0, start.1, &timezones)?;
                    let end = parse_icalendar_datetime(end.0, end.1, &timezones)?;
                    if end <= start {
                        return None;
                    }
                    (Some(start), Some(end), None, None, None, None)
                }
                _ => return None,
            }
        };
    let organizer = optional_single_icalendar_identity(event, "ORGANIZER")?;
    Some(CalendarMeetingResponse {
        method,
        transport_attachment_id: None,
        server_processed: false,
        organizer,
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

fn is_false(value: &bool) -> bool {
    !*value
}

fn unfold_icalendar_lines(value: &str) -> Vec<String> {
    let mut lines: Vec<String> = Vec::new();
    for line in value.lines().map(|line| line.trim_end_matches('\r')) {
        if (line.starts_with(' ') || line.starts_with('\t')) && !lines.is_empty() {
            let continuation = line
                .strip_prefix(' ')
                .or_else(|| line.strip_prefix('\t'))
                .expect("iCalendar continuation starts with whitespace");
            lines
                .last_mut()
                .expect("iCalendar line exists")
                .push_str(continuation);
        } else {
            lines.push(line.to_string());
        }
    }
    lines
}

fn icalendar_property<'a>(line: &'a str, name: &str) -> Option<(&'a str, &'a str)> {
    let separator = icalendar_unquoted_separator(line, ':')?;
    let (left, value) = line.split_at(separator);
    let value = value.get(1..)?;
    let (property_name, parameters) = left.split_once(';').unwrap_or((left, ""));
    property_name
        .eq_ignore_ascii_case(name)
        .then_some((parameters, value))
}

fn icalendar_unquoted_separator(value: &str, separator: char) -> Option<usize> {
    let mut quoted = false;
    for (index, character) in value.char_indices() {
        match character {
            '"' => quoted = !quoted,
            character if character == separator && !quoted => return Some(index),
            _ => {}
        }
    }
    None
}

fn icalendar_value(lines: &[String], name: &str) -> Option<String> {
    lines
        .iter()
        .find_map(|line| icalendar_property(line, name).map(|(_, value)| value.trim().to_string()))
}

fn optional_single_icalendar_property<'a>(
    lines: &'a [String],
    name: &str,
) -> Option<Option<(&'a str, &'a str)>> {
    let mut properties = lines
        .iter()
        .filter_map(|line| icalendar_property(line, name));
    let first = properties.next();
    properties.next().is_none().then_some(first)
}

fn single_icalendar_property<'a>(lines: &'a [String], name: &str) -> Option<(&'a str, &'a str)> {
    optional_single_icalendar_property(lines, name)?
}

fn parse_icalendar_uid(value: &str) -> Option<String> {
    let value = unescape_icalendar_text(value.trim())?;
    if value.is_empty()
        || value.chars().any(char::is_control)
        || calendar_uid_has_occurrence_date(&value)
    {
        return None;
    }
    Some(value)
}

fn unescape_icalendar_text(value: &str) -> Option<String> {
    let mut unescaped = String::with_capacity(value.len());
    let mut characters = value.chars();
    while let Some(character) = characters.next() {
        if character != '\\' {
            unescaped.push(character);
            continue;
        }
        match characters.next()? {
            '\\' => unescaped.push('\\'),
            ',' => unescaped.push(','),
            ';' => unescaped.push(';'),
            'n' | 'N' => unescaped.push('\n'),
            _ => return None,
        }
    }
    Some(unescaped)
}

fn optional_icalendar_sequence(lines: &[String]) -> Option<Option<i32>> {
    let outlook_sequence =
        optional_single_icalendar_property(lines, "X-MICROSOFT-CDO-APPT-SEQUENCE")?;
    let standard_sequence = optional_single_icalendar_property(lines, "SEQUENCE")?;
    let parse_sequence = |property: Option<(&str, &str)>| -> Option<Option<i32>> {
        match property {
            Some((_, value)) => Some(Some(i32::try_from(value.trim().parse::<u32>().ok()?).ok()?)),
            None => Some(None),
        }
    };
    match (
        parse_sequence(outlook_sequence)?,
        parse_sequence(standard_sequence)?,
    ) {
        (Some(outlook), Some(standard)) if outlook == standard => Some(Some(outlook)),
        (Some(_), Some(_)) => None,
        (Some(sequence), None) | (None, Some(sequence)) => Some(Some(sequence)),
        (None, None) => Some(None),
    }
}

fn icalendar_parameter(parameters: &str, name: &str) -> Option<String> {
    let mut remaining = parameters;
    loop {
        let separator = icalendar_unquoted_separator(remaining, ';');
        let (parameter, next) = match separator {
            Some(separator) => {
                let (parameter, next) = remaining.split_at(separator);
                (parameter, next.get(1..).unwrap_or_default())
            }
            None => (remaining, ""),
        };
        if let Some((key, value)) = parameter.split_once('=') {
            if key.eq_ignore_ascii_case(name) {
                return Some(value.trim_matches('"').to_string());
            }
        }
        if next.is_empty() {
            return None;
        }
        remaining = next;
    }
}

fn optional_single_icalendar_identity(
    lines: &[String],
    property_name: &str,
) -> Option<Option<CalendarMeetingIdentity>> {
    let mut identities = lines
        .iter()
        .filter_map(|line| icalendar_property(line, property_name));
    let Some((parameters, value)) = identities.next() else {
        return Some(None);
    };
    if identities.next().is_some() {
        return None;
    }
    Some(Some(CalendarMeetingIdentity {
        email: icalendar_mailto_address(value)?,
        display_name: icalendar_parameter(parameters, "CN").unwrap_or_default(),
    }))
}

fn icalendar_attendees(lines: &[String]) -> Vec<CalendarMeetingAttendee> {
    lines
        .iter()
        .filter_map(|line| icalendar_property(line, "ATTENDEE"))
        .filter_map(|(parameters, value)| {
            Some(CalendarMeetingAttendee {
                email: icalendar_mailto_address(value)?,
                display_name: icalendar_parameter(parameters, "CN").unwrap_or_default(),
                cutype: icalendar_parameter(parameters, "CUTYPE")
                    .unwrap_or_else(|| "INDIVIDUAL".to_string())
                    .to_ascii_uppercase(),
                role: icalendar_parameter(parameters, "ROLE")
                    .unwrap_or_else(|| "REQ-PARTICIPANT".to_string())
                    .to_ascii_uppercase(),
                partstat: icalendar_parameter(parameters, "PARTSTAT")
                    .unwrap_or_else(|| "NEEDS-ACTION".to_string())
                    .to_ascii_lowercase(),
                rsvp: icalendar_parameter(parameters, "RSVP")
                    .is_some_and(|value| value.eq_ignore_ascii_case("TRUE")),
            })
        })
        .collect()
}

fn icalendar_mailto_address(value: &str) -> Option<String> {
    let value = value.trim();
    let prefix = value.get(.."mailto:".len())?;
    if !prefix.eq_ignore_ascii_case("mailto:") {
        return None;
    }
    let address = crate::normalize_email(value.get("mailto:".len()..)?);
    (!address.is_empty()).then_some(address)
}

pub fn parse_message_attachments(bytes: &[u8]) -> Result<Vec<AttachmentUploadInput>> {
    collect_mime_attachment_parts(bytes)?
        .into_iter()
        .enumerate()
        .map(|(index, mut attachment)| {
            let is_calendar = attachment
                .declared_mime
                .as_deref()
                .is_some_and(is_text_calendar_media_type);
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
                is_scheduling_body: attachment.is_scheduling_body,
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
    let from_headers = parse_header_records(bytes)
        .into_iter()
        .filter(|header| header.name.eq_ignore_ascii_case("from"))
        .collect::<Vec<_>>();
    let from_is_unambiguous =
        from_headers.len() == 1 && parse_address_list(&from_headers[0].value).len() == 1;

    Ok(ParsedRfc822Message {
        from: headers
            .get("from")
            .and_then(|value| parse_address_list(value).into_iter().next()),
        from_is_unambiguous,
        sender: headers
            .get("sender")
            .and_then(|value| parse_address_list(value).into_iter().next()),
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
    let mut addresses = Vec::new();
    let mut start = 0usize;
    let mut quoted = false;
    let mut escaped = false;
    let mut angle_depth = 0u32;
    for (index, character) in value.char_indices() {
        if escaped {
            escaped = false;
            continue;
        }
        match character {
            '\\' if quoted => escaped = true,
            '"' => quoted = !quoted,
            '<' if !quoted => angle_depth = angle_depth.saturating_add(1),
            '>' if !quoted => angle_depth = angle_depth.saturating_sub(1),
            ',' if !quoted && angle_depth == 0 => {
                if let Some(address) = parse_single_address(&value[start..index]) {
                    addresses.push(address);
                }
                start = index + character.len_utf8();
            }
            _ => {}
        }
    }
    if let Some(address) = parse_single_address(&value[start..]) {
        addresses.push(address);
    }
    addresses
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
        parse_calendar_meeting_request, parse_calendar_meeting_response, parse_header_recipients,
        parse_icalendar_uid, parse_message_attachments, parse_message_date_header,
        parse_rfc822_message, unfold_icalendar_lines,
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
    fn parse_header_recipients_does_not_split_a_quoted_display_name() {
        let raw = concat!(
            "To: \"Doe, Alice\" <ALICE@EXAMPLE.TEST>, Bob <bob@example.test>\r\n",
            "\r\n",
            "Body\r\n"
        );

        let to = parse_header_recipients(raw.as_bytes(), "to");

        assert_eq!(to.len(), 2);
        assert_eq!(to[0].address, "alice@example.test");
        assert_eq!(to[0].display_name.as_deref(), Some("Doe, Alice"));
        assert_eq!(to[1].address, "bob@example.test");
        assert_eq!(to[1].display_name.as_deref(), Some("Bob"));
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
            "ORGANIZER;CN=Denis Ducret:mailto:denis.ducret@sdic.ch\r\n",
            "ATTENDEE;CN=Observer;CUTYPE=RESOURCE;ROLE=NON-PARTICIPANT;PARTSTAT=ACCEPTED;RSVP=FALSE:mailto:observer@example.test\r\n",
            "ATTENDEE;CN=LPE Test;ROLE=OPT-PARTICIPANT;PARTSTAT=TENTATIVE;RSVP=TRUE:mailto:test@l-p-e.ch\r\n",
            "END:VEVENT\r\n",
            "END:VCALENDAR\r\n",
            "--invite--\r\n"
        );

        let attachments = parse_message_attachments(message.as_bytes()).unwrap();

        assert_eq!(attachments.len(), 1);
        assert_eq!(attachments[0].file_name, "invite.ics");
        assert!(attachments[0].is_scheduling_body);
        assert_eq!(
            attachments[0].media_type,
            "text/calendar; method=REQUEST; charset=UTF-8"
        );
        assert_eq!(
            super::parse_calendar_meeting_request(&attachments),
            Some(super::CalendarMeetingRequest {
                uid: "probe-7@example.test".to_string(),
                transport_attachment_id: None,
                organizer: Some(super::CalendarMeetingIdentity {
                    email: "denis.ducret@sdic.ch".to_string(),
                    display_name: "Denis Ducret".to_string(),
                }),
                attendees: vec![
                    super::CalendarMeetingAttendee {
                        email: "observer@example.test".to_string(),
                        display_name: "Observer".to_string(),
                        cutype: "RESOURCE".to_string(),
                        role: "NON-PARTICIPANT".to_string(),
                        partstat: "accepted".to_string(),
                        rsvp: false,
                    },
                    super::CalendarMeetingAttendee {
                        email: "test@l-p-e.ch".to_string(),
                        display_name: "LPE Test".to_string(),
                        cutype: "INDIVIDUAL".to_string(),
                        role: "OPT-PARTICIPANT".to_string(),
                        partstat: "tentative".to_string(),
                        rsvp: true,
                    },
                ],
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
            is_scheduling_body: true,
            blob_bytes: body.as_bytes().to_vec(),
        };
        assert_eq!(
            super::parse_calendar_meeting_request(&[attachment(
                "text/calendar-evil; method=REQUEST",
                "BEGIN:VCALENDAR\r\nMETHOD:REQUEST\r\nBEGIN:VEVENT\r\nUID:x\r\nDTSTART:20260824T063000Z\r\nDTEND:20260824T070000Z\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
            )]),
            None
        );
        assert_eq!(
            super::parse_calendar_meeting_request(&[attachment(
                "text/calendar; method=REQUEST",
                "BEGIN:VCALENDAR\r\nMETHOD:PUBLISH\r\nBEGIN:VEVENT\r\nUID:x\r\nDTSTART:20260824T063000Z\r\nDTEND:20260824T070000Z\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
            )]),
            None
        );

        let mut explicit_attachment = attachment(
            "text/calendar; method=REQUEST",
            "BEGIN:VCALENDAR\r\nMETHOD:REQUEST\r\nBEGIN:VEVENT\r\nUID:x\r\nDTSTART:20260824T063000Z\r\nDTEND:20260824T070000Z\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
        );
        explicit_attachment.disposition = Some("attachment".to_string());
        explicit_attachment.is_scheduling_body = false;
        assert_eq!(
            super::parse_calendar_meeting_request(&[explicit_attachment]),
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
        assert_eq!(
            super::parse_calendar_meeting_request(&[attachment(
                "text/calendar; method=REQUEST",
                "BEGIN:VCALENDAR\r\nMETHOD:REQUEST\r\nBEGIN:VEVENT\r\nUID:040000008200E00074C5B7101A82E00807EA0818C08470CD9E31DD01000000000000000010000000ECFF8AEC00CE584390F914BF6A87F955\r\nDTSTART:20260825T063000Z\r\nDTEND:20260825T070000Z\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
            )]),
            None
        );
        assert!(super::parse_calendar_meeting_request(&[attachment(
            "text/calendar; method=REQUEST",
            "BEGIN:VCALENDAR\r\nMETHOD:REQUEST\r\nBEGIN:VEVENT\r\nUID:first\r\nUID:second\r\nDTSTART:20260825T063000Z\r\nDTEND:20260825T070000Z\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
        )])
        .is_none());
        assert!(super::parse_calendar_meeting_request(&[attachment(
            "text/calendar; method=REQUEST",
            "BEGIN:VCALENDAR\r\nMETHOD:REQUEST\r\nBEGIN:VEVENT\r\nUID:x\r\nDTSTAMP:20260824T060000Z\r\nDTSTAMP:20260824T070000Z\r\nDTSTART:20260825T063000Z\r\nDTEND:20260825T070000Z\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
        )])
        .is_none());
        assert!(super::parse_calendar_meeting_request(&[attachment(
            "text/calendar; method=REQUEST",
            "BEGIN:VCALENDAR\r\nMETHOD:REQUEST\r\nBEGIN:VEVENT\r\nUID:x\r\nDTSTART:20260825T063000Z\r\nDTEND:20260825T070000Z\r\nSEQUENCE:1\r\nX-MICROSOFT-CDO-APPT-SEQUENCE:2\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
        )])
        .is_none());
        assert_eq!(
            super::parse_calendar_meeting_request(&[attachment(
                "text/calendar; method=REQUEST",
                "BEGIN:VCALENDAR\r\nMETHOD:REQUEST\r\nBEGIN:VEVENT\r\nUID:x\r\nDTSTART:20260825T063000Z\r\nDTEND:20260825T070000Z\r\nSEQUENCE:1\r\nX-MICROSOFT-CDO-APPT-SEQUENCE:1\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
            )])
            .map(|request| request.meeting_sequence),
            Some(1)
        );
    }

    #[test]
    fn calendar_meeting_request_resolves_outlook_daylight_timezone() {
        let attachments = |start: &str, end: &str| {
            let message = format!(
                concat!(
                    "MIME-Version: 1.0\r\n",
                    "Content-Type: multipart/alternative; boundary=invite\r\n",
                    "\r\n",
                    "--invite\r\n",
                    "Content-Type: text/plain; charset=utf-8\r\n",
                    "\r\n",
                    "Probe 10\r\n",
                    "--invite\r\n",
                    "Content-Type: text/calendar; method=REQUEST; charset=utf-8\r\n",
                    "\r\n",
                    "BEGIN:VCALENDAR\r\n",
                    "METHOD:REQUEST\r\n",
                    "BEGIN:VTIMEZONE\r\n",
                    "TZID:W. Europe Standard Time\r\n",
                    "BEGIN:STANDARD\r\n",
                    "DTSTART:16010101T030000\r\n",
                    "TZOFFSETFROM:+0200\r\n",
                    "TZOFFSETTO:+0100\r\n",
                    "RRULE:FREQ=YEARLY;BYMONTH=10;BYDAY=-1SU\r\n",
                    "END:STANDARD\r\n",
                    "BEGIN:DAYLIGHT\r\n",
                    "DTSTART:16010101T020000\r\n",
                    "TZOFFSETFROM:+0100\r\n",
                    "TZOFFSETTO:+0200\r\n",
                    "RRULE:FREQ=YEARLY;BYMONTH=3;BYDAY=-1SU\r\n",
                    "END:DAYLIGHT\r\n",
                    "END:VTIMEZONE\r\n",
                    "BEGIN:VEVENT\r\n",
                    "UID:probe-10@example.test\r\n",
                    "DTSTAMP:20260824T060000Z\r\n",
                    "DTSTART;TZID=\"W. Europe Standard Time\":{start}\r\n",
                    "DTEND;TZID=\"W. Europe Standard Time\":{end}\r\n",
                    "ATTENDEE;RSVP=TRUE:mailto:test@l-p-e.ch\r\n",
                    "END:VEVENT\r\n",
                    "END:VCALENDAR\r\n",
                    "--invite--\r\n"
                ),
                start = start,
                end = end,
            );
            super::parse_message_attachments(message.as_bytes())
                .expect("Outlook request MIME should parse")
        };

        let summer_parts = attachments("20260824T083000", "20260824T090000");
        assert_eq!(summer_parts.len(), 1);
        assert_eq!(summer_parts[0].file_name, "calendar-1.ics");
        assert_eq!(summer_parts[0].disposition, None);
        assert!(summer_parts[0].is_scheduling_body);
        let summer = super::parse_calendar_meeting_request(&summer_parts)
            .expect("Outlook summer request should parse");
        assert_eq!(summer.meeting_start, "2026-08-24T06:30:00Z");
        assert_eq!(summer.meeting_end, "2026-08-24T07:00:00Z");

        let winter = super::parse_calendar_meeting_request(&attachments(
            "20260124T083000",
            "20260124T090000",
        ))
        .expect("Outlook winter request should parse");
        assert_eq!(winter.meeting_start, "2026-01-24T07:30:00Z");
        assert_eq!(winter.meeting_end, "2026-01-24T08:00:00Z");

        assert!(super::parse_calendar_meeting_request(&attachments(
            "20260329T023000",
            "20260329T033000",
        ))
        .is_none());
        assert!(super::parse_calendar_meeting_request(&attachments(
            "20261025T023000",
            "20261025T033000",
        ))
        .is_none());
    }

    #[test]
    fn parse_message_attachments_names_unnamed_scheduling_calendar_parts() {
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
        assert_eq!(attachments[0].disposition, None);
        assert!(attachments[0].is_scheduling_body);
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
                transport_attachment_id: None,
                server_processed: false,
                organizer: None,
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
    fn parse_calendar_meeting_response_preserves_quoted_parameter_separators() {
        let attachments = parse_message_attachments(
            concat!(
                "Content-Type: text/calendar; method=REPLY; charset=UTF-8\r\n",
                "\r\n",
                "BEGIN:VCALENDAR\r\n",
                "METHOD:REPLY\r\n",
                "BEGIN:VEVENT\r\n",
                "UID:quoted-parameters@example.test\r\n",
                "DTSTAMP:20260824T080000Z\r\n",
                "ORGANIZER;CN=\"Team: Europe; West\":mailto:organizer@example.test\r\n",
                "ATTENDEE;CN=\"Ducret: Denis; Sales\";PARTSTAT=ACCEPTED:mailto:denis.ducret@sdic.ch\r\n",
                "END:VEVENT\r\n",
                "END:VCALENDAR\r\n",
            )
            .as_bytes(),
        )
        .unwrap();

        let response = parse_calendar_meeting_response(&attachments).unwrap();
        assert_eq!(
            response.organizer.unwrap().display_name,
            "Team: Europe; West"
        );
        assert_eq!(response.attendee_name, "Ducret: Denis; Sales");
        assert_eq!(response.attendee_email, "denis.ducret@sdic.ch");
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
                transport_attachment_id: None,
                server_processed: false,
                organizer: None,
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
    fn parse_calendar_meeting_response_rejects_invalid_organizers_and_intervals() {
        let parse = |method: &str, event: &str| {
            let raw = format!(
                "Content-Type: text/calendar; method={method}; charset=UTF-8\r\n\r\nBEGIN:VCALENDAR\r\nMETHOD:{method}\r\nBEGIN:VEVENT\r\n{event}END:VEVENT\r\nEND:VCALENDAR\r\n"
            );
            let attachments = parse_message_attachments(raw.as_bytes()).unwrap();
            parse_calendar_meeting_response(&attachments)
        };
        let reply = concat!(
            "ATTENDEE;PARTSTAT=ACCEPTED:mailto:denis.ducret@sdic.ch\r\n",
            "DTSTART:20260824T063000Z\r\n",
            "DTEND:20260824T070000Z\r\n",
            "UID:probe@example.test\r\n",
        );

        assert!(parse(
            "REPLY",
            &format!("ORGANIZER:https://example.test/calendar\r\n{reply}")
        )
        .is_none());
        assert!(parse(
            "REPLY",
            &format!(
                "ORGANIZER:mailto:test@l-p-e.ch\r\nORGANIZER:mailto:other@example.test\r\n{reply}"
            )
        )
        .is_none());
        assert!(parse(
            "REPLY",
            concat!(
                "ATTENDEE;PARTSTAT=ACCEPTED:mailto:denis.ducret@sdic.ch\r\n",
                "DTSTART:20260824T070000Z\r\n",
                "DTEND:20260824T063000Z\r\n",
                "UID:probe@example.test\r\n",
            )
        )
        .is_none());
        assert!(parse(
            "COUNTER",
            concat!(
                "ATTENDEE;PARTSTAT=TENTATIVE:mailto:denis.ducret@sdic.ch\r\n",
                "DTSTART:20260824T070000Z\r\n",
                "DTEND:20260824T063000Z\r\n",
                "UID:probe@example.test\r\n",
            )
        )
        .is_none());
        assert!(parse(
            "COUNTER",
            concat!(
                "ATTENDEE;PARTSTAT=TENTATIVE:mailto:denis.ducret@sdic.ch\r\n",
                "DTSTART:20260824T073000Z\r\n",
                "DTEND:20260824T080000Z\r\n",
                "X-MS-OLK-ORIGINALSTART:20260824T070000Z\r\n",
                "X-MS-OLK-ORIGINALEND:20260824T063000Z\r\n",
                "UID:probe@example.test\r\n",
            )
        )
        .is_none());
    }

    #[test]
    fn parse_calendar_meeting_response_rejects_duplicate_singleton_properties() {
        let parse = |method: &str, event: &str| {
            let raw = format!(
                "Content-Type: text/calendar; method={method}; charset=UTF-8\r\n\r\nBEGIN:VCALENDAR\r\nMETHOD:{method}\r\nBEGIN:VEVENT\r\n{event}END:VEVENT\r\nEND:VCALENDAR\r\n"
            );
            let attachments = parse_message_attachments(raw.as_bytes()).unwrap();
            parse_calendar_meeting_response(&attachments)
        };
        let attendee = "ATTENDEE;PARTSTAT=ACCEPTED:mailto:denis.ducret@sdic.ch\r\n";

        assert!(parse(
            "REPLY",
            &format!("{attendee}UID:first@example.test\r\nUID:second@example.test\r\n")
        )
        .is_none());
        assert!(parse(
            "REPLY",
            &format!(
                "{attendee}DTSTAMP:20260821T170000Z\r\nDTSTAMP:20260821T180000Z\r\nUID:probe@example.test\r\n"
            )
        )
        .is_none());
        assert!(parse(
            "REPLY",
            &format!(
                "{attendee}DTSTART:20260824T063000Z\r\nDTSTART:20260824T070000Z\r\nDTEND:20260824T073000Z\r\nUID:probe@example.test\r\n"
            )
        )
        .is_none());
        assert!(parse(
            "REPLY",
            &format!(
                "{attendee}SEQUENCE:1\r\nX-MICROSOFT-CDO-APPT-SEQUENCE:2\r\nUID:probe@example.test\r\n"
            )
        )
        .is_none());
        assert!(parse(
            "REPLY",
            &format!("{attendee}LOCATION:First\r\nLOCATION:Second\r\nUID:probe@example.test\r\n")
        )
        .is_none());

        assert!(parse(
            "REPLY",
            &format!(
                "{attendee}SEQUENCE:0\r\nX-MICROSOFT-CDO-APPT-SEQUENCE:0\r\nUID:probe@example.test\r\n"
            )
        )
        .is_some());
    }

    #[test]
    fn normalizes_mapi_global_object_id_calendar_uids() {
        let encoded = "040000008200E00074C5B7101A82E00800000000C08470CD9E31DD01000000000000000010000000ECFF8AEC00CE584390F914BF6A87F955";
        let canonical = format!("mapi-goid:{}", encoded.to_ascii_lowercase());
        assert_eq!(
            super::normalize_calendar_meeting_uid(&format!(" MAPI-GOID:{encoded} ")),
            canonical
        );
        assert_eq!(super::normalize_calendar_meeting_uid(encoded), canonical);
        assert_eq!(
            super::normalize_calendar_meeting_uid(&format!(
                "mapi-goid:{}",
                encoded.to_ascii_lowercase()
            )),
            canonical
        );
        assert_eq!(
            super::normalize_calendar_meeting_uid(&encoded.to_ascii_lowercase()),
            encoded.to_ascii_lowercase()
        );
        assert_eq!(
            super::normalize_calendar_meeting_uid(" MAPI-GOID:0011aAbb "),
            "MAPI-GOID:0011aAbb"
        );
        let inconsistent_size = format!("{}11000000{}", &encoded[..72], &encoded[80..]);
        assert_eq!(
            super::normalize_calendar_meeting_uid(&inconsistent_size),
            inconsistent_size
        );
        let nonzero_reserved_x = format!("{}0100000000000000{}", &encoded[..56], &encoded[72..]);
        assert_eq!(
            super::normalize_calendar_meeting_uid(&nonzero_reserved_x),
            nonzero_reserved_x
        );
        assert_eq!(
            super::normalize_calendar_meeting_uid("Opaque-Uid@Example.Test"),
            "Opaque-Uid@Example.Test"
        );
    }

    #[test]
    fn meeting_calendar_uid_text_escapes_round_trip_for_requests_and_responses() {
        let escaped_uid = r"third\,party\;team\\slot@example.test";
        let expected_uid = r"third,party;team\slot@example.test";
        let request_attachments = parse_message_attachments(
            format!(
                concat!(
                    "Content-Type: text/calendar; method=REQUEST; charset=UTF-8\r\n",
                    "\r\n",
                    "BEGIN:VCALENDAR\r\n",
                    "METHOD:REQUEST\r\n",
                    "BEGIN:VEVENT\r\n",
                    "UID:{escaped_uid}\r\n",
                    "DTSTART:20260825T080000Z\r\n",
                    "DTEND:20260825T083000Z\r\n",
                    "ATTENDEE;RSVP=TRUE:mailto:attendee@example.test\r\n",
                    "END:VEVENT\r\n",
                    "END:VCALENDAR\r\n"
                ),
                escaped_uid = escaped_uid
            )
            .as_bytes(),
        )
        .unwrap();
        let response_attachments = parse_message_attachments(
            format!(
                concat!(
                    "Content-Type: text/calendar; method=REPLY; charset=UTF-8\r\n",
                    "\r\n",
                    "BEGIN:VCALENDAR\r\n",
                    "METHOD:REPLY\r\n",
                    "BEGIN:VEVENT\r\n",
                    "UID:{escaped_uid}\r\n",
                    "ATTENDEE;PARTSTAT=ACCEPTED:mailto:attendee@example.test\r\n",
                    "END:VEVENT\r\n",
                    "END:VCALENDAR\r\n"
                ),
                escaped_uid = escaped_uid
            )
            .as_bytes(),
        )
        .unwrap();

        assert_eq!(
            parse_calendar_meeting_request(&request_attachments)
                .unwrap()
                .uid,
            expected_uid
        );
        assert_eq!(
            parse_calendar_meeting_response(&response_attachments)
                .unwrap()
                .uid,
            expected_uid
        );
    }

    #[test]
    fn meeting_calendar_uid_rejects_invalid_text_escapes_and_controls() {
        for uid in [r"invalid\q@example.test", r"invalid\n@example.test"] {
            assert!(parse_icalendar_uid(uid).is_none());
        }
    }

    #[test]
    fn icalendar_unfolding_removes_exactly_one_continuation_octet() {
        assert_eq!(
            unfold_icalendar_lines("UID:first\r\n  second\r\n\t third"),
            vec!["UID:first second third".to_string()]
        );
    }

    #[test]
    fn calendar_meeting_response_rejects_recurrence_id_until_occurrence_correlation_exists() {
        let attachments = parse_message_attachments(
            concat!(
                "Content-Type: text/calendar; method=REPLY; charset=UTF-8\r\n",
                "\r\n",
                "BEGIN:VCALENDAR\r\n",
                "METHOD:REPLY\r\n",
                "BEGIN:VEVENT\r\n",
                "ATTENDEE;PARTSTAT=ACCEPTED:mailto:denis.ducret@sdic.ch\r\n",
                "RECURRENCE-ID:20260825T080000Z\r\n",
                "UID:recurring-probe@example.test\r\n",
                "END:VEVENT\r\n",
                "END:VCALENDAR\r\n"
            )
            .as_bytes(),
        )
        .unwrap();

        assert!(parse_calendar_meeting_response(&attachments).is_none());

        let encoded_occurrence = parse_message_attachments(
            concat!(
                "Content-Type: text/calendar; method=REPLY; charset=UTF-8\r\n",
                "\r\n",
                "BEGIN:VCALENDAR\r\n",
                "METHOD:REPLY\r\n",
                "BEGIN:VEVENT\r\n",
                "ATTENDEE;PARTSTAT=ACCEPTED:mailto:denis.ducret@sdic.ch\r\n",
                "UID:mapi-goid:040000008200e00074c5b7101a82e00807ea0818c08470cd9e31dd01000000000000000010000000ecff8aec00ce584390f914bf6a87f955\r\n",
                "END:VEVENT\r\n",
                "END:VCALENDAR\r\n"
            )
            .as_bytes(),
        )
        .unwrap();
        assert!(parse_calendar_meeting_response(&encoded_occurrence).is_none());
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
                transport_attachment_id: None,
                server_processed: false,
                organizer: None,
                attendee_email: "denis.ducret@sdic.ch".to_string(),
                attendee_name: String::new(),
                partstat: "accepted".to_string(),
                uid: "MAPI-GOID:001122".to_string(),
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
    fn parse_calendar_meeting_response_accepts_case_insensitive_components() {
        let attachments = parse_message_attachments(
            concat!(
                "Content-Type: text/calendar; method=REPLY; charset=UTF-8\r\n",
                "\r\n",
                "begin:vcalendar\r\n",
                "method:REPLY\r\n",
                "begin:vevent\r\n",
                "attendee;partstat=ACCEPTED:mailto:denis.ducret@sdic.ch\r\n",
                "uid:case-insensitive@example.test\r\n",
                "end:vevent\r\n",
                "end:vcalendar\r\n"
            )
            .as_bytes(),
        )
        .unwrap();

        assert!(parse_calendar_meeting_response(&attachments).is_some());
    }

    #[test]
    fn parse_calendar_meeting_response_rejects_conflicting_mime_method() {
        let attachments = parse_message_attachments(
            concat!(
                "Content-Type: text/calendar; method=REQUEST; charset=UTF-8\r\n",
                "\r\n",
                "BEGIN:VCALENDAR\r\n",
                "METHOD:REPLY\r\n",
                "BEGIN:VEVENT\r\n",
                "ATTENDEE;PARTSTAT=ACCEPTED:mailto:denis.ducret@sdic.ch\r\n",
                "UID:mapi-goid:001122\r\n",
                "END:VEVENT\r\n",
                "END:VCALENDAR\r\n"
            )
            .as_bytes(),
        )
        .unwrap();

        assert!(parse_calendar_meeting_response(&attachments).is_none());
    }

    #[test]
    fn calendar_parsers_reject_lookalike_media_types() {
        let attachment = crate::AttachmentUploadInput {
            file_name: "response.ics".to_string(),
            media_type: "text/calendar-evil; method=REPLY".to_string(),
            disposition: Some("inline".to_string()),
            content_id: None,
            is_scheduling_body: true,
            blob_bytes: concat!(
                "BEGIN:VCALENDAR\r\n",
                "METHOD:REPLY\r\n",
                "BEGIN:VEVENT\r\n",
                "ATTENDEE;PARTSTAT=ACCEPTED:mailto:denis.ducret@sdic.ch\r\n",
                "UID:mapi-goid:001122\r\n",
                "END:VEVENT\r\n",
                "END:VCALENDAR\r\n"
            )
            .as_bytes()
            .to_vec(),
        };

        assert!(!super::is_text_calendar_media_type(&attachment.media_type));
        assert!(parse_calendar_meeting_response(&[attachment]).is_none());
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
            "Sender: Delivery Agent <agent@example.test>\r\n",
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
        assert!(parsed.from_is_unambiguous);
        assert_eq!(parsed.message_id.as_deref(), Some("<id@example.test>"));
        assert_eq!(parsed.from.as_ref().unwrap().email, "alice@example.test");
        assert_eq!(
            parsed.from.as_ref().unwrap().display_name.as_deref(),
            Some("Alice")
        );
        assert_eq!(parsed.sender.as_ref().unwrap().email, "agent@example.test");
        assert_eq!(
            parsed.sender.as_ref().unwrap().display_name.as_deref(),
            Some("Delivery Agent")
        );
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
    fn parse_rfc822_message_selects_the_first_from_mailbox() {
        let message = concat!(
            "From: \"Doe, Alice\" <ALICE@EXAMPLE.TEST>, Bob <bob@example.test>\r\n",
            "Sender: Delivery Agent <agent@example.test>\r\n",
            "Subject: Multiple From mailboxes\r\n",
            "Content-Type: text/plain; charset=utf-8\r\n",
            "\r\n",
            "Body\r\n"
        );

        let parsed = parse_rfc822_message(message.as_bytes()).unwrap();
        assert!(!parsed.from_is_unambiguous);
        let from = parsed.from.expect("first From mailbox should parse");

        assert_eq!(from.email, "alice@example.test");
        assert_eq!(from.display_name.as_deref(), Some("Doe, Alice"));
    }

    #[test]
    fn parse_rfc822_message_marks_duplicate_from_headers_as_ambiguous() {
        let message = concat!(
            "From: Alice <alice@example.test>\r\n",
            "From: Forged <forged@example.test>\r\n",
            "Subject: Duplicate From\r\n",
            "Content-Type: text/plain; charset=utf-8\r\n",
            "\r\n",
            "Body\r\n"
        );

        let parsed = parse_rfc822_message(message.as_bytes()).unwrap();

        assert!(!parsed.from_is_unambiguous);
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
