use anyhow::{anyhow, bail, Result};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use lpe_storage::{
    parse_calendar_participants_metadata, ActiveSyncAttachment, CalendarParticipantMetadata,
    ClientContact, ClientEvent, JmapEmail, JmapUploadBlob,
};
use serde_json::{json, Value};
use std::collections::HashMap;
use uuid::Uuid;

use crate::{
    constants::MAIL_CLASS,
    message::{activesync_timestamp, format_email_address, split_name},
    protocol::BodyPreferenceType,
    types::{CollectionDefinition, CollectionStateEntry, SnapshotChange, SnapshotEntry},
    wbxml::WbxmlNode,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BodyPreference {
    pub(crate) body_type: BodyPreferenceType,
    pub(crate) truncation_size: Option<usize>,
}

impl Default for BodyPreference {
    fn default() -> Self {
        Self {
            body_type: BodyPreferenceType::PlainText,
            truncation_size: None,
        }
    }
}

pub(crate) fn email_application_data(
    email: &JmapEmail,
    attachments: &[ActiveSyncAttachment],
    body_preference: &BodyPreference,
    mime_blob: Option<&JmapUploadBlob>,
) -> Value {
    let to = email
        .to
        .iter()
        .map(format_email_address)
        .collect::<Vec<_>>()
        .join(", ");
    let cc = email
        .cc
        .iter()
        .map(format_email_address)
        .collect::<Vec<_>>()
        .join(", ");

    let mut children = vec![
        json!({"page": 2, "name": "Subject", "text": email.subject}),
        json!({"page": 2, "name": "From", "text": email.from_display.as_deref().map(|display| format!("{display} <{}>", email.from_address)).unwrap_or_else(|| email.from_address.clone())}),
        json!({"page": 2, "name": "To", "text": to}),
        json!({"page": 2, "name": "Cc", "text": cc}),
        json!({"page": 2, "name": "DisplayTo", "text": to}),
        json!({"page": 2, "name": "Read", "text": if email.unread { "0" } else { "1" }}),
        json!({"page": 2, "name": "Importance", "text": "1"}),
        json!({"page": 2, "name": "MessageClass", "text": "IPM.Note"}),
        json!({"page": 2, "name": "DateReceived", "text": activesync_timestamp(email.sent_at.as_deref().unwrap_or(&email.received_at))}),
        email_body_value(email, body_preference, mime_blob),
    ];

    if !attachments.is_empty() {
        children.push(json!({
            "page": 17,
            "name": "Attachments",
            "children": attachments.iter().map(|attachment| json!({
                "page": 17,
                "name": "Attachment",
                "children": [
                    {"page": 17, "name": "DisplayName", "text": attachment.file_name},
                    {"page": 17, "name": "FileReference", "text": attachment.file_reference},
                    {"page": 17, "name": "Method", "text": "1"},
                    {"page": 17, "name": "ContentType", "text": attachment.media_type},
                    {"page": 17, "name": "EstimatedDataSize", "text": attachment.size_octets.to_string()},
                    {"page": 17, "name": "IsInline", "text": "0"}
                ]
            })).collect::<Vec<_>>()
        }));
    }
    if let Some(flag) = email_flag_value(email) {
        children.push(flag);
    }

    json!({
        "page": 0,
        "name": "ApplicationData",
        "children": children,
    })
}

fn email_flag_value(email: &JmapEmail) -> Option<Value> {
    if email.followup_flag_status == "none" && !email.flagged {
        return None;
    }

    let status = if email.followup_flag_status == "complete" {
        "1"
    } else {
        "2"
    };
    let mut children = vec![
        json!({"page": 2, "name": "Status", "text": status}),
        json!({"page": 2, "name": "FlagType", "text": "Flag for follow up"}),
    ];

    if let (Some(start_at), Some(due_at)) = (&email.followup_start_at, &email.followup_due_at) {
        children
            .push(json!({"page": 9, "name": "StartDate", "text": activesync_timestamp(start_at)}));
        children.push(
            json!({"page": 9, "name": "UtcStartDate", "text": activesync_timestamp(start_at)}),
        );
        children.push(json!({"page": 9, "name": "DueDate", "text": activesync_timestamp(due_at)}));
        children
            .push(json!({"page": 9, "name": "UtcDueDate", "text": activesync_timestamp(due_at)}));
    }
    if let Some(completed_at) = &email.followup_completed_at {
        let completed = activesync_timestamp(completed_at);
        children.push(json!({"page": 2, "name": "CompleteTime", "text": completed}));
        children.push(json!({"page": 9, "name": "DateCompleted", "text": completed}));
    }

    Some(json!({
        "page": 2,
        "name": "Flag",
        "children": children,
    }))
}

fn email_body_value(
    email: &JmapEmail,
    body_preference: &BodyPreference,
    mime_blob: Option<&JmapUploadBlob>,
) -> Value {
    let (body_type, bytes) = match body_preference.body_type {
        BodyPreferenceType::Html => match email.body_html_sanitized.as_ref() {
            Some(html) => (BodyPreferenceType::Html, html.as_bytes().to_vec()),
            None => (
                BodyPreferenceType::PlainText,
                email.body_text.as_bytes().to_vec(),
            ),
        },
        BodyPreferenceType::Mime => match mime_blob {
            Some(blob) => (BodyPreferenceType::Mime, blob.blob_bytes.clone()),
            None => (
                BodyPreferenceType::PlainText,
                email.body_text.as_bytes().to_vec(),
            ),
        },
        BodyPreferenceType::PlainText => (
            BodyPreferenceType::PlainText,
            email.body_text.as_bytes().to_vec(),
        ),
    };
    let estimated_size = bytes.len();
    let (data, truncated) = truncate_body_bytes(&bytes, body_preference.truncation_size);
    let data_node = if body_type == BodyPreferenceType::Mime {
        json!({"page": 17, "name": "Data", "opaque_base64": BASE64.encode(data)})
    } else {
        json!({"page": 17, "name": "Data", "text": String::from_utf8_lossy(&data)})
    };
    json!({
        "page": 17,
        "name": "Body",
        "children": [
            {"page": 17, "name": "Type", "text": body_type.as_str()},
            {"page": 17, "name": "EstimatedDataSize", "text": estimated_size.to_string()},
            data_node,
            {"page": 17, "name": "Truncated", "text": if truncated { "1" } else { "0" }}
        ]
    })
}

fn truncate_body_bytes(bytes: &[u8], truncation_size: Option<usize>) -> (Vec<u8>, bool) {
    let Some(limit) = truncation_size else {
        return (bytes.to_vec(), false);
    };
    if bytes.len() <= limit {
        return (bytes.to_vec(), false);
    }
    (bytes[..limit].to_vec(), true)
}

pub(crate) fn contact_application_data(contact: &ClientContact) -> Value {
    let (first_name, last_name) = split_name(&contact.name);
    let mut children = Vec::new();
    push_text(&mut children, 1, "FileAs", &contact.name);
    push_text(&mut children, 1, "FirstName", &first_name);
    push_text(&mut children, 1, "LastName", &last_name);
    push_text(&mut children, 1, "Email1Address", &contact.email);
    push_text(&mut children, 1, "MobilePhoneNumber", &contact.phone);
    push_text(&mut children, 1, "BusinessPhoneNumber", &contact.phone);
    push_text(&mut children, 1, "CompanyName", &contact.team);
    push_text(&mut children, 1, "JobTitle", &contact.role);
    push_body(&mut children, &contact.notes);

    json!({
        "page": 0,
        "name": "ApplicationData",
        "children": children
    })
}

pub(crate) fn calendar_application_data(event: &ClientEvent) -> Value {
    let mut children = Vec::new();
    push_text(&mut children, 4, "UID", &event.uid);
    push_text(&mut children, 4, "TimeZone", &event.time_zone);
    push_text(&mut children, 4, "Subject", &event.title);
    push_text(
        &mut children,
        4,
        "StartTime",
        &compact_datetime(&event.date, &event.time),
    );
    push_text(
        &mut children,
        4,
        "EndTime",
        &add_minutes_to_compact(&event.date, &event.time, event.duration_minutes),
    );
    push_text(&mut children, 4, "Location", &event.location);

    let participants = parse_calendar_participants_metadata(&event.attendees_json);
    if let Some(organizer) = participants.organizer.as_ref() {
        push_text(&mut children, 4, "OrganizerName", &organizer.common_name);
        push_text(&mut children, 4, "OrganizerEmail", &organizer.email);
    }
    push_attendees(&mut children, &participants.attendees);
    push_body(&mut children, &event.notes);
    if let Some(recurrence) = recurrence_application_data(&event.recurrence_rule) {
        children.push(recurrence);
    }

    json!({
        "page": 0,
        "name": "ApplicationData",
        "children": children
    })
}

fn push_text(children: &mut Vec<Value>, page: u8, name: &str, value: &str) {
    if !value.trim().is_empty() {
        children.push(json!({"page": page, "name": name, "text": value}));
    }
}

fn push_body(children: &mut Vec<Value>, value: &str) {
    if value.trim().is_empty() {
        return;
    }
    children.push(json!({
        "page": 17,
        "name": "Body",
        "children": [
            {"page": 17, "name": "Type", "text": BodyPreferenceType::PlainText.as_str()},
            {"page": 17, "name": "EstimatedDataSize", "text": value.len().to_string()},
            {"page": 17, "name": "Data", "text": value},
            {"page": 17, "name": "Truncated", "text": "0"}
        ]
    }));
}

fn push_attendees(children: &mut Vec<Value>, attendees: &[CalendarParticipantMetadata]) {
    let attendee_nodes = attendees
        .iter()
        .map(|attendee| {
            let mut fields = Vec::new();
            push_text(&mut fields, 4, "Name", &attendee.common_name);
            push_text(&mut fields, 4, "Email", &attendee.email);
            push_text(
                &mut fields,
                4,
                "AttendeeType",
                if attendee.role.eq_ignore_ascii_case("OPT-PARTICIPANT") {
                    "2"
                } else {
                    "1"
                },
            );
            push_text(
                &mut fields,
                4,
                "AttendeeStatus",
                attendee_status(&attendee.partstat),
            );
            json!({"page": 4, "name": "Attendee", "children": fields})
        })
        .collect::<Vec<_>>();
    if !attendee_nodes.is_empty() {
        children.push(json!({"page": 4, "name": "Attendees", "children": attendee_nodes}));
    }
}

fn attendee_status(partstat: &str) -> &'static str {
    match partstat.trim().to_ascii_lowercase().as_str() {
        "tentative" => "2",
        "accepted" => "3",
        "declined" => "4",
        _ => "5",
    }
}

fn compact_datetime(date: &str, time: &str) -> String {
    format!("{}T{}00Z", date.replace('-', ""), time.replace(':', ""))
}

fn add_minutes_to_compact(date: &str, time: &str, duration_minutes: i32) -> String {
    let Some((year, month, day)) = parse_date(date) else {
        return compact_datetime(date, time);
    };
    let Some((hour, minute)) = parse_time(time) else {
        return compact_datetime(date, time);
    };
    let total = days_from_civil(year, month, day) * 1440
        + i64::from(hour) * 60
        + i64::from(minute)
        + i64::from(duration_minutes.max(0));
    let (end_year, end_month, end_day) = civil_from_days(total.div_euclid(1440));
    let minute_of_day = total.rem_euclid(1440);
    format!(
        "{end_year:04}{end_month:02}{end_day:02}T{:02}{:02}00Z",
        minute_of_day / 60,
        minute_of_day % 60
    )
}

fn parse_date(value: &str) -> Option<(i64, i64, i64)> {
    let mut parts = value.split('-');
    let year = parts.next()?.parse().ok()?;
    let month = parts.next()?.parse().ok()?;
    let day = parts.next()?.parse().ok()?;
    if parts.next().is_some() {
        return None;
    }
    Some((year, month, day))
}

fn parse_time(value: &str) -> Option<(i64, i64)> {
    let mut parts = value.split(':');
    let hour = parts.next()?.parse().ok()?;
    let minute = parts.next()?.parse().ok()?;
    Some((hour, minute))
}

fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let year = year - if month <= 2 { 1 } else { 0 };
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let yoe = year - era * 400;
    let month_prime = month + if month > 2 { -3 } else { 9 };
    let doy = (153 * month_prime + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

fn civil_from_days(days: i64) -> (i64, i64, i64) {
    let days = days + 719468;
    let era = if days >= 0 { days } else { days - 146096 } / 146097;
    let doe = days - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let year = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = mp + if mp < 10 { 3 } else { -9 };
    (year + if month <= 2 { 1 } else { 0 }, month, day)
}

fn recurrence_application_data(rrule: &str) -> Option<Value> {
    let fields = rrule_fields(rrule);
    let freq = fields.get("FREQ")?;
    let mut children = Vec::new();
    match freq.as_str() {
        "DAILY" => push_text(&mut children, 4, "Type", "0"),
        "WEEKLY" => {
            push_text(&mut children, 4, "Type", "1");
            if let Some(day_of_week) = fields.get("BYDAY").and_then(|value| {
                let mask = value
                    .split(',')
                    .filter_map(rrule_weekday_mask)
                    .fold(0u32, |acc, value| acc | value);
                (mask > 0).then_some(mask.to_string())
            }) {
                push_text(&mut children, 4, "DayOfWeek", &day_of_week);
            }
        }
        "MONTHLY" => {
            let day = fields.get("BYMONTHDAY")?;
            push_text(&mut children, 4, "Type", "2");
            push_text(&mut children, 4, "DayOfMonth", day);
        }
        "YEARLY" => {
            let day = fields.get("BYMONTHDAY")?;
            let month = fields.get("BYMONTH")?;
            push_text(&mut children, 4, "Type", "5");
            push_text(&mut children, 4, "DayOfMonth", day);
            push_text(&mut children, 4, "MonthOfYear", month);
        }
        _ => return None,
    }
    if let Some(interval) = fields.get("INTERVAL") {
        push_text(&mut children, 4, "Interval", interval);
    }
    if let Some(count) = fields.get("COUNT") {
        push_text(&mut children, 4, "Occurrences", count);
    }
    if let Some(until) = fields.get("UNTIL") {
        push_text(&mut children, 4, "Until", &rrule_until_to_compact(until));
    }
    Some(json!({"page": 4, "name": "Recurrence", "children": children}))
}

fn rrule_fields(rrule: &str) -> HashMap<String, String> {
    rrule
        .split(';')
        .filter_map(|part| part.split_once('='))
        .map(|(key, value)| {
            (
                key.trim().to_ascii_uppercase(),
                value.trim().to_ascii_uppercase(),
            )
        })
        .collect()
}

fn rrule_weekday_mask(value: &str) -> Option<u32> {
    match value.trim().to_ascii_uppercase().as_str() {
        "SU" => Some(1),
        "MO" => Some(2),
        "TU" => Some(4),
        "WE" => Some(8),
        "TH" => Some(16),
        "FR" => Some(32),
        "SA" => Some(64),
        _ => None,
    }
}

fn rrule_until_to_compact(value: &str) -> String {
    let date = value.split('T').next().unwrap_or(value);
    if date.len() == 8 {
        format!("{date}T000000Z")
    } else {
        value.to_string()
    }
}

pub(crate) fn snapshot_to_value(entries: &[SnapshotEntry]) -> Value {
    Value::Array(
        entries
            .iter()
            .map(|entry| {
                json!({
                    "id": entry.server_id,
                    "fingerprint": entry.fingerprint,
                    "data": entry.data,
                })
            })
            .collect(),
    )
}

pub(crate) fn diff_snapshots(previous: Option<&Value>, current: &Value) -> Vec<SnapshotChange> {
    let previous_fingerprints = snapshot_fingerprints(previous);
    let current_fingerprints = snapshot_fingerprints(Some(current));
    let mut changes = Vec::new();

    for (server_id, fingerprint) in &current_fingerprints {
        match previous_fingerprints.get(server_id) {
            None => changes.push(SnapshotChange {
                kind: "Add".to_string(),
                server_id: server_id.clone(),
            }),
            Some(previous) if previous != fingerprint => changes.push(SnapshotChange {
                kind: "Update".to_string(),
                server_id: server_id.clone(),
            }),
            _ => {}
        }
    }

    for server_id in previous_fingerprints.keys() {
        if !current_fingerprints.contains_key(server_id) {
            changes.push(SnapshotChange {
                kind: "Delete".to_string(),
                server_id: server_id.clone(),
            });
        }
    }

    changes.sort_by(|left, right| left.server_id.cmp(&right.server_id));
    changes
}

pub(crate) fn diff_collection_states(
    previous: &[CollectionStateEntry],
    current: &[CollectionStateEntry],
) -> Vec<SnapshotChange> {
    let previous_fingerprints = previous
        .iter()
        .map(|entry| (entry.server_id.clone(), entry.fingerprint.clone()))
        .collect::<HashMap<_, _>>();
    let current_fingerprints = current
        .iter()
        .map(|entry| (entry.server_id.clone(), entry.fingerprint.clone()))
        .collect::<HashMap<_, _>>();
    let mut changes = Vec::new();

    for (server_id, fingerprint) in &current_fingerprints {
        match previous_fingerprints.get(server_id) {
            None => changes.push(SnapshotChange {
                kind: "Add".to_string(),
                server_id: server_id.clone(),
            }),
            Some(previous) if previous != fingerprint => changes.push(SnapshotChange {
                kind: "Update".to_string(),
                server_id: server_id.clone(),
            }),
            _ => {}
        }
    }

    for server_id in previous_fingerprints.keys() {
        if !current_fingerprints.contains_key(server_id) {
            changes.push(SnapshotChange {
                kind: "Delete".to_string(),
                server_id: server_id.clone(),
            });
        }
    }

    changes.sort_by(|left, right| left.server_id.cmp(&right.server_id));
    changes
}

fn snapshot_fingerprints(snapshot: Option<&Value>) -> HashMap<String, String> {
    let mut map = HashMap::new();
    if let Some(Value::Array(entries)) = snapshot {
        for entry in entries {
            if let Some(object) = entry.as_object() {
                let id = object
                    .get("id")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string();
                let fingerprint = object
                    .get("fingerprint")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string();
                map.insert(id, fingerprint);
            }
        }
    }
    map
}

pub(crate) fn value_to_node(data: &serde_json::Map<String, Value>) -> WbxmlNode {
    let page = data.get("page").and_then(Value::as_u64).unwrap_or(0) as u8;
    let name = data
        .get("name")
        .and_then(Value::as_str)
        .unwrap_or("ApplicationData");
    let mut node = WbxmlNode::new(page, name);
    node.text = data
        .get("text")
        .and_then(Value::as_str)
        .map(ToString::to_string);
    node.opaque = data
        .get("opaque_base64")
        .and_then(Value::as_str)
        .and_then(|value| BASE64.decode(value).ok());
    if let Some(Value::Array(children)) = data.get("children") {
        for child in children {
            if let Some(object) = child.as_object() {
                node.push(value_to_node(object));
            }
        }
    }
    node
}

pub(crate) fn collection_window_size(sync: &WbxmlNode, collection: &WbxmlNode) -> u64 {
    let window_size = collection
        .child("WindowSize")
        .or_else(|| sync.child("WindowSize"))
        .map(|node| node.text_value().trim().to_string())
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(250);
    window_size.clamp(1, 512)
}

pub(crate) fn require_collection_id(collection_node: &WbxmlNode) -> Result<String> {
    collection_node
        .child("CollectionId")
        .map(|node| node.text_value().trim().to_string())
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow!("Sync collection is missing CollectionId"))
}

pub(crate) fn require_sync_collections(request: &WbxmlNode) -> Result<Vec<WbxmlNode>> {
    if request.name != "Sync" {
        bail!("invalid Sync payload");
    }
    let Some(collections) = request.child("Collections") else {
        bail!("Sync request must include one collection");
    };
    let children = collections.children_named("Collection");
    if children.is_empty() {
        bail!("Sync request must include one collection");
    }
    Ok(children.into_iter().cloned().collect())
}

pub(crate) fn mail_collection(collection: &CollectionDefinition) -> bool {
    collection.class_name == MAIL_CLASS
}

pub(crate) fn drafts_collection(collection: &CollectionDefinition) -> bool {
    collection.class_name == MAIL_CLASS && collection.display_name == "Drafts"
}

pub(crate) fn parse_collection_mailbox_id(collection: &CollectionDefinition) -> Result<Uuid> {
    collection
        .mailbox_id
        .ok_or_else(|| anyhow!("mailbox missing"))
}
