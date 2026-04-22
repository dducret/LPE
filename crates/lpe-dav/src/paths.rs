use crate::{
    serialize::{serialize_ical, serialize_vcard, serialize_vtodo},
};
use lpe_storage::{AccessibleContact, AccessibleEvent, DavTask};
use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
};
use uuid::Uuid;

pub(crate) const ROOT_PATH: &str = "/dav/";
pub(crate) const PRINCIPAL_PATH: &str = "/dav/principals/me/";
pub(crate) const ADDRESSBOOK_HOME_PATH: &str = "/dav/addressbooks/me/";
pub(crate) const DEFAULT_COLLECTION_ID: &str = "default";
pub(crate) const ADDRESSBOOK_COLLECTION_PREFIX: &str = "/dav/addressbooks/me/";
pub(crate) const ADDRESSBOOK_COLLECTION_PATH: &str = "/dav/addressbooks/me/default/";
pub(crate) const CALENDAR_HOME_PATH: &str = "/dav/calendars/me/";
pub(crate) const CALENDAR_COLLECTION_PREFIX: &str = "/dav/calendars/me/";
pub(crate) const CALENDAR_COLLECTION_PATH: &str = "/dav/calendars/me/default/";
pub(crate) const TASK_COLLECTION_PREFIX: &str = "tasks-";

pub(crate) fn normalized_path(path: &str) -> String {
    match path {
        "/dav" => ROOT_PATH.to_string(),
        other => other.to_string(),
    }
}

pub(crate) fn contact_collection_href(collection_id: &str) -> String {
    format!("{ADDRESSBOOK_COLLECTION_PREFIX}{collection_id}/")
}

pub(crate) fn event_collection_href(collection_id: &str) -> String {
    format!("{CALENDAR_COLLECTION_PREFIX}{collection_id}/")
}

pub(crate) fn dav_task_collection_id(collection_id: &str) -> String {
    format!("{TASK_COLLECTION_PREFIX}{collection_id}")
}

pub(crate) fn task_collection_id_from_path(path: &str) -> Option<String> {
    let collection_id = collection_id_from_path(path, CALENDAR_COLLECTION_PREFIX)?;
    collection_id
        .strip_prefix(TASK_COLLECTION_PREFIX)
        .map(ToString::to_string)
}

pub(crate) fn task_collection_href(collection_id: &str) -> String {
    event_collection_href(&dav_task_collection_id(collection_id))
}

pub(crate) fn contact_href(collection_id: &str, id: Uuid) -> String {
    format!("{}{id}.vcf", contact_collection_href(collection_id))
}

pub(crate) fn event_href(collection_id: &str, id: Uuid) -> String {
    format!("{}{id}.ics", event_collection_href(collection_id))
}

pub(crate) fn task_href(collection_id: &str, id: Uuid) -> String {
    format!("{}{id}.ics", task_collection_href(collection_id))
}

pub(crate) fn collection_id_from_contact_path(path: &str) -> Option<String> {
    collection_id_from_path(path, ADDRESSBOOK_COLLECTION_PREFIX)
}

pub(crate) fn collection_id_from_event_path(path: &str) -> Option<String> {
    let collection_id = collection_id_from_path(path, CALENDAR_COLLECTION_PREFIX)?;
    if collection_id.starts_with(TASK_COLLECTION_PREFIX) {
        return None;
    }
    Some(collection_id)
}

pub(crate) fn collection_id_from_path(path: &str, prefix: &str) -> Option<String> {
    let rest = path.strip_prefix(prefix)?;
    let collection_id = rest.split('/').next()?.trim();
    if collection_id.is_empty() {
        return None;
    }
    Some(collection_id.to_string())
}

pub(crate) fn resource_id_for_contact_path(path: &str) -> Option<(String, Uuid)> {
    resource_id_for_path(path, ADDRESSBOOK_COLLECTION_PREFIX, ".vcf")
}

pub(crate) fn resource_id_for_event_path(path: &str) -> Option<(String, Uuid)> {
    resource_id_for_path(path, CALENDAR_COLLECTION_PREFIX, ".ics")
}

pub(crate) fn resource_id_for_task_path(path: &str) -> Option<(String, Uuid)> {
    let (collection_id, resource_id) =
        resource_id_for_path(path, CALENDAR_COLLECTION_PREFIX, ".ics")?;
    collection_id
        .strip_prefix(TASK_COLLECTION_PREFIX)
        .map(|collection_id| (collection_id.to_string(), resource_id))
}

pub(crate) fn resource_id_for_path(
    path: &str,
    prefix: &str,
    suffix: &str,
) -> Option<(String, Uuid)> {
    let rest = path.strip_prefix(prefix)?;
    let (collection_id, file_name) = rest.split_once('/')?;
    if collection_id.is_empty() {
        return None;
    }
    let resource_id = file_name
        .strip_suffix(suffix)
        .and_then(parse_uuid_path_segment)?;
    Some((collection_id.to_string(), resource_id))
}

pub(crate) fn parse_uuid_path_segment(value: &str) -> Option<Uuid> {
    if value.contains('/') || value.is_empty() {
        return None;
    }
    Uuid::parse_str(value).ok()
}

pub(crate) fn etag(value: &str) -> String {
    let mut hasher = DefaultHasher::new();
    value.hash(&mut hasher);
    format!("\"{:x}\"", hasher.finish())
}

pub(crate) fn etag_for_contact(contact: &AccessibleContact) -> String {
    etag(&serialize_vcard(contact))
}

pub(crate) fn etag_for_event(event: &AccessibleEvent) -> String {
    etag(&serialize_ical(event))
}

pub(crate) fn etag_for_task(task: &DavTask) -> String {
    etag(&serialize_vtodo(task))
}
