use anyhow::{anyhow, bail, Result};
use lpe_storage::{ClientContact, ClientEvent, JmapEmail};
use serde_json::{json, Value};
use std::collections::HashMap;
use uuid::Uuid;

use crate::{
    constants::{CALENDAR_CLASS, CONTACTS_CLASS, MAIL_CLASS},
    message::{activesync_timestamp, format_email_address, split_name},
    types::{CollectionDefinition, SnapshotChange, SnapshotEntry},
    wbxml::WbxmlNode,
};

pub(crate) fn fingerprint_email(email: &JmapEmail) -> String {
    let recipients = |values: &[lpe_storage::JmapEmailAddress]| {
        values
            .iter()
            .map(|recipient| recipient.address.as_str())
            .collect::<Vec<_>>()
            .join(",")
    };
    format!(
        "{}|{}|{}|{}|{}|{}|{}|{}|{}",
        email.subject,
        email.preview,
        email.body_text,
        email
            .sent_at
            .clone()
            .unwrap_or_else(|| email.received_at.clone()),
        email.unread,
        email.flagged,
        recipients(&email.to),
        recipients(&email.cc),
        recipients(&email.bcc),
    )
}

pub(crate) fn fingerprint_contact(contact: &ClientContact) -> String {
    format!(
        "{}|{}|{}|{}|{}|{}",
        contact.name, contact.role, contact.email, contact.phone, contact.team, contact.notes
    )
}

pub(crate) fn fingerprint_event(event: &ClientEvent) -> String {
    format!(
        "{}|{}|{}|{}|{}|{}",
        event.date, event.time, event.title, event.location, event.attendees, event.notes
    )
}

pub(crate) fn email_application_data(email: &JmapEmail) -> Value {
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

    json!({
        "page": 0,
        "name": "ApplicationData",
        "children": [
            {"page": 2, "name": "Subject", "text": email.subject},
            {"page": 2, "name": "From", "text": email.from_display.as_deref().map(|display| format!("{display} <{}>", email.from_address)).unwrap_or_else(|| email.from_address.clone())},
            {"page": 2, "name": "To", "text": to},
            {"page": 2, "name": "Cc", "text": cc},
            {"page": 2, "name": "DisplayTo", "text": to},
            {"page": 2, "name": "Read", "text": if email.unread { "0" } else { "1" }},
            {"page": 2, "name": "Importance", "text": "1"},
            {"page": 2, "name": "MessageClass", "text": "IPM.Note"},
            {"page": 2, "name": "DateReceived", "text": activesync_timestamp(email.sent_at.as_deref().unwrap_or(&email.received_at))},
            {
                "page": 17,
                "name": "Body",
                "children": [
                    {"page": 17, "name": "Type", "text": "1"},
                    {"page": 17, "name": "EstimatedDataSize", "text": email.body_text.len().to_string()},
                    {"page": 17, "name": "Data", "text": email.body_text},
                    {"page": 17, "name": "Truncated", "text": "0"}
                ]
            }
        ]
    })
}

pub(crate) fn contact_application_data(contact: &ClientContact) -> Value {
    let (first_name, last_name) = split_name(&contact.name);
    json!({
        "page": 0,
        "name": "ApplicationData",
        "children": [
            {"page": 1, "name": "FileAs", "text": contact.name},
            {"page": 1, "name": "FirstName", "text": first_name},
            {"page": 1, "name": "LastName", "text": last_name},
            {"page": 1, "name": "Email1Address", "text": contact.email},
            {"page": 1, "name": "MobilePhoneNumber", "text": contact.phone},
            {"page": 1, "name": "HomePhoneNumber", "text": contact.phone}
        ]
    })
}

pub(crate) fn calendar_application_data(event: &ClientEvent) -> Value {
    json!({
        "page": 0,
        "name": "ApplicationData",
        "children": [
            {"page": 4, "name": "Subject", "text": event.title},
            {"page": 4, "name": "StartTime", "text": format!("{}T{}:00Z", event.date.replace('-', ""), event.time.replace(':', ""))},
            {"page": 4, "name": "EndTime", "text": format!("{}T{}:00Z", event.date.replace('-', ""), event.time.replace(':', ""))},
            {"page": 4, "name": "Location", "text": event.location},
            {"page": 4, "name": "OrganizerName", "text": event.attendees},
            {"page": 4, "name": "OrganizerEmail", "text": ""},
            {
                "page": 17,
                "name": "Body",
                "children": [
                    {"page": 17, "name": "Type", "text": "1"},
                    {"page": 17, "name": "EstimatedDataSize", "text": event.notes.len().to_string()},
                    {"page": 17, "name": "Data", "text": event.notes},
                    {"page": 17, "name": "Truncated", "text": "0"}
                ]
            }
        ]
    })
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

pub(crate) fn snapshot_map(snapshot: &Value) -> HashMap<String, WbxmlNode> {
    let mut map = HashMap::new();
    if let Value::Array(entries) = snapshot {
        for entry in entries {
            if let Some(object) = entry.as_object() {
                let id = object
                    .get("id")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string();
                if let Some(data) = object.get("data").and_then(Value::as_object) {
                    map.insert(id, value_to_node(data));
                }
            }
        }
    }
    map
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

pub(crate) fn collection_entries(
    collection: &CollectionDefinition,
    emails: Vec<JmapEmail>,
    contacts: Vec<ClientContact>,
    events: Vec<ClientEvent>,
) -> Result<Value> {
    match collection.class_name.as_str() {
        MAIL_CLASS => Ok(snapshot_to_value(
            &emails
                .into_iter()
                .map(|email| SnapshotEntry {
                    server_id: email.id.to_string(),
                    fingerprint: fingerprint_email(&email),
                    data: email_application_data(&email),
                })
                .collect::<Vec<_>>(),
        )),
        CONTACTS_CLASS => Ok(snapshot_to_value(
            &contacts
                .into_iter()
                .map(|contact| SnapshotEntry {
                    server_id: contact.id.to_string(),
                    fingerprint: fingerprint_contact(&contact),
                    data: contact_application_data(&contact),
                })
                .collect::<Vec<_>>(),
        )),
        CALENDAR_CLASS => Ok(snapshot_to_value(
            &events
                .into_iter()
                .map(|event| SnapshotEntry {
                    server_id: event.id.to_string(),
                    fingerprint: fingerprint_event(&event),
                    data: calendar_application_data(&event),
                })
                .collect::<Vec<_>>(),
        )),
        _ => bail!("unsupported collection class"),
    }
}

pub(crate) fn parse_collection_mailbox_id(collection: &CollectionDefinition) -> Result<Uuid> {
    collection
        .mailbox_id
        .ok_or_else(|| anyhow!("mailbox missing"))
}
