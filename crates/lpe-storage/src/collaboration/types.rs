use anyhow::{bail, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::{
    CollaborationCollectionRow, CollaborationGrantRow, DEFAULT_COLLECTION_ID,
    DEFAULT_CONTACT_BOOK_ROLE, IM_CONTACT_LIST_COLLECTION_ID, IM_CONTACT_LIST_ROLE,
    QUICK_CONTACTS_COLLECTION_ID, QUICK_CONTACTS_ROLE, SUGGESTED_CONTACTS_COLLECTION_ID,
    SUGGESTED_CONTACTS_ROLE,
};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum CollaborationResourceKind {
    Contacts,
    Calendar,
    Tasks,
}

impl CollaborationResourceKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Contacts => "contacts",
            Self::Calendar => "calendar",
            Self::Tasks => "tasks",
        }
    }

    pub fn collection_label(&self) -> &'static str {
        match self {
            Self::Contacts => "Contacts",
            Self::Calendar => "Calendar",
            Self::Tasks => "Task List",
        }
    }
}

#[derive(Debug, Clone, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CollaborationRights {
    pub may_read: bool,
    pub may_write: bool,
    pub may_delete: bool,
    pub may_share: bool,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CollaborationCollection {
    pub id: String,
    pub kind: String,
    pub owner_account_id: Uuid,
    pub owner_email: String,
    pub owner_display_name: String,
    pub display_name: String,
    pub is_owned: bool,
    pub rights: CollaborationRights,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ContactNameFields {
    pub prefix: String,
    pub given: String,
    pub middle: String,
    pub family: String,
    pub suffix: String,
    pub nickname: String,
    pub phonetic_given: String,
    pub phonetic_family: String,
}

impl Default for ContactNameFields {
    fn default() -> Self {
        Self {
            prefix: String::new(),
            given: String::new(),
            middle: String::new(),
            family: String::new(),
            suffix: String::new(),
            nickname: String::new(),
            phonetic_given: String::new(),
            phonetic_family: String::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ContactSourceFields {
    pub import_source: String,
    pub source_uid: Option<String>,
    pub source_etag: Option<String>,
    pub source_payload_json: Value,
}

impl Default for ContactSourceFields {
    fn default() -> Self {
        Self {
            import_source: "local".to_string(),
            source_uid: None,
            source_etag: None,
            source_payload_json: Value::Object(Default::default()),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AccessibleContact {
    pub id: Uuid,
    pub collection_id: String,
    pub owner_account_id: Uuid,
    pub owner_email: String,
    pub owner_display_name: String,
    pub rights: CollaborationRights,
    pub name: String,
    pub role: String,
    pub email: String,
    pub phone: String,
    pub team: String,
    pub notes: String,
    pub structured_name: ContactNameFields,
    pub emails_json: Value,
    pub phones_json: Value,
    pub addresses_json: Value,
    pub urls_json: Value,
    pub organization_name: String,
    pub job_title: String,
    pub raw_vcard: Option<String>,
    pub source: ContactSourceFields,
}

impl Default for AccessibleContact {
    fn default() -> Self {
        Self {
            id: Uuid::nil(),
            collection_id: String::new(),
            owner_account_id: Uuid::nil(),
            owner_email: String::new(),
            owner_display_name: String::new(),
            rights: CollaborationRights::default(),
            name: String::new(),
            role: String::new(),
            email: String::new(),
            phone: String::new(),
            team: String::new(),
            notes: String::new(),
            structured_name: ContactNameFields::default(),
            emails_json: Value::Array(Vec::new()),
            phones_json: Value::Array(Vec::new()),
            addresses_json: Value::Array(Vec::new()),
            urls_json: Value::Array(Vec::new()),
            organization_name: String::new(),
            job_title: String::new(),
            raw_vcard: None,
            source: ContactSourceFields::default(),
        }
    }
}

impl AccessibleContact {
    pub fn primary_email(&self) -> &str {
        &self.email
    }

    pub fn primary_phone(&self) -> &str {
        &self.phone
    }

    pub fn display_name(&self) -> &str {
        &self.name
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AccessibleEvent {
    pub id: Uuid,
    pub uid: String,
    pub collection_id: String,
    pub owner_account_id: Uuid,
    pub owner_email: String,
    pub owner_display_name: String,
    pub rights: CollaborationRights,
    pub date: String,
    pub time: String,
    pub time_zone: String,
    pub duration_minutes: i32,
    pub all_day: bool,
    pub status: String,
    pub sequence: i32,
    pub recurrence_rule: String,
    pub recurrence_json: String,
    pub recurrence_exceptions_json: String,
    pub title: String,
    pub location: String,
    pub organizer_json: String,
    pub attendees: String,
    pub attendees_json: String,
    pub notes: String,
    pub body_html: String,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct MapiEventIdentityMove {
    pub account_id: Uuid,
    pub old_mapi_object_id: u64,
    pub new_mapi_object_id: u64,
    pub old_source_key: Vec<u8>,
    pub new_source_key: Vec<u8>,
    pub old_change_number: u64,
    pub new_change_number: u64,
    pub old_change_key: Vec<u8>,
    pub new_change_key: Vec<u8>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MoveAccessibleEventToDeletedItemsResult {
    pub event: AccessibleEvent,
    pub principal_identity: Option<MapiEventIdentityMove>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CollaborationGrant {
    pub id: Uuid,
    pub kind: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub calendar_id: Option<Uuid>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub calendar_name: Option<String>,
    pub owner_account_id: Uuid,
    pub owner_email: String,
    pub owner_display_name: String,
    pub grantee_account_id: Uuid,
    pub grantee_email: String,
    pub grantee_display_name: String,
    pub rights: CollaborationRights,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct FreeBusyBlock {
    pub owner_account_id: Uuid,
    pub owner_email: String,
    pub start: String,
    pub end: String,
    pub status: String,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DelegateAccessObject {
    pub owner_account_id: Uuid,
    pub owner_email: String,
    pub grantee_account_id: Uuid,
    pub grantee_email: String,
    pub can_view_free_busy: bool,
    pub can_open_calendar: bool,
    pub can_create_or_update_calendar_items: bool,
    pub can_delete_calendar_items: bool,
    pub can_receive_meeting_objects: bool,
    pub can_send_on_behalf: bool,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DelegateFreeBusyMessageObject {
    pub id: Uuid,
    pub account_id: Uuid,
    pub owner_account_id: Uuid,
    pub owner_email: String,
    pub message_kind: String,
    pub subject: String,
    pub body_text: String,
    pub starts_at: Option<String>,
    pub ends_at: Option<String>,
    pub busy_status: Option<String>,
    pub payload_json: String,
    pub updated_at: String,
}

#[derive(Debug, Clone)]
pub struct CollaborationGrantInput {
    pub kind: CollaborationResourceKind,
    pub owner_account_id: Uuid,
    pub grantee_email: String,
    pub calendar_id: Option<Uuid>,
    pub may_read: bool,
    pub may_write: bool,
    pub may_delete: bool,
    pub may_share: bool,
}

pub(crate) fn validate_collaboration_rights(
    may_read: bool,
    may_write: bool,
    may_delete: bool,
    may_share: bool,
) -> Result<()> {
    if !may_read && (may_write || may_delete || may_share) {
        bail!("read access is required when granting write, delete, or share");
    }
    if may_delete && !may_write {
        bail!("delete access requires write access");
    }
    if may_share && !may_write {
        bail!("share access requires write access");
    }
    Ok(())
}

pub(super) fn collection_id_for_owner(
    kind: CollaborationResourceKind,
    principal_account_id: Uuid,
    owner_account_id: Uuid,
    role: &str,
) -> String {
    if principal_account_id == owner_account_id {
        if kind == CollaborationResourceKind::Contacts && role == SUGGESTED_CONTACTS_ROLE {
            SUGGESTED_CONTACTS_COLLECTION_ID.to_string()
        } else if kind == CollaborationResourceKind::Contacts && role == QUICK_CONTACTS_ROLE {
            QUICK_CONTACTS_COLLECTION_ID.to_string()
        } else if kind == CollaborationResourceKind::Contacts && role == IM_CONTACT_LIST_ROLE {
            IM_CONTACT_LIST_COLLECTION_ID.to_string()
        } else {
            DEFAULT_COLLECTION_ID.to_string()
        }
    } else {
        shared_collection_id(kind, owner_account_id)
    }
}

pub(super) fn calendar_collection_id_for_event(
    principal_account_id: Uuid,
    owner_account_id: Uuid,
    calendar_id: Uuid,
    role: &str,
) -> String {
    if role == "custom" {
        calendar_id.to_string()
    } else {
        collection_id_for_owner(
            CollaborationResourceKind::Calendar,
            principal_account_id,
            owner_account_id,
            role,
        )
    }
}

pub(super) fn contact_book_role_for_collection_id(collection_id: Option<&str>) -> &'static str {
    match collection_id.map(str::trim) {
        Some(SUGGESTED_CONTACTS_COLLECTION_ID) => SUGGESTED_CONTACTS_ROLE,
        Some(QUICK_CONTACTS_COLLECTION_ID) => QUICK_CONTACTS_ROLE,
        Some(IM_CONTACT_LIST_COLLECTION_ID) => IM_CONTACT_LIST_ROLE,
        _ => DEFAULT_CONTACT_BOOK_ROLE,
    }
}

fn shared_collection_id(kind: CollaborationResourceKind, owner_account_id: Uuid) -> String {
    format!("shared-{}-{}", kind.as_str(), owner_account_id)
}

pub(super) fn shared_collection_id_for_row(
    kind: CollaborationResourceKind,
    row: &CollaborationCollectionRow,
) -> String {
    if kind == CollaborationResourceKind::Calendar && row.role == "custom" {
        row.id.to_string()
    } else {
        shared_collection_id(kind, row.owner_account_id)
    }
}

pub(super) fn shared_collection_display_name(
    kind: CollaborationResourceKind,
    owner_display_name: &str,
    owner_email: &str,
) -> String {
    let owner_label = if owner_display_name.trim().is_empty() {
        owner_email.trim()
    } else {
        owner_display_name.trim()
    };
    format!("{owner_label} {}", kind.collection_label())
}

pub(super) fn map_collaboration_grant(row: CollaborationGrantRow) -> CollaborationGrant {
    CollaborationGrant {
        id: row.id,
        kind: row.kind,
        calendar_id: row.calendar_id,
        calendar_name: row.calendar_name,
        owner_account_id: row.owner_account_id,
        owner_email: row.owner_email,
        owner_display_name: row.owner_display_name,
        grantee_account_id: row.grantee_account_id,
        grantee_email: row.grantee_email,
        grantee_display_name: row.grantee_display_name,
        rights: CollaborationRights {
            may_read: row.may_read,
            may_write: row.may_write,
            may_delete: row.may_delete,
            may_share: row.may_share,
        },
        created_at: row.created_at,
        updated_at: row.updated_at,
    }
}

pub(super) fn merge_free_busy_rows(
    rows: Vec<crate::FreeBusyEventRow>,
    owner_account_id: Uuid,
    owner_email: String,
    can_read_details: bool,
) -> Vec<FreeBusyBlock> {
    let mut blocks: Vec<FreeBusyBlock> = Vec::new();
    for row in rows {
        let status = free_busy_status(&row.status, can_read_details);
        if status == "free" || row.starts_at >= row.ends_at {
            continue;
        }
        if let Some(last) = blocks.last_mut() {
            if last.status == status && last.end >= row.starts_at {
                if last.end < row.ends_at {
                    last.end = row.ends_at;
                }
                continue;
            }
        }
        blocks.push(FreeBusyBlock {
            owner_account_id,
            owner_email: owner_email.clone(),
            start: row.starts_at,
            end: row.ends_at,
            status,
        });
    }
    blocks
}

fn free_busy_status(status: &str, can_read_details: bool) -> String {
    if status.trim().eq_ignore_ascii_case("cancelled") {
        return "free".to_string();
    }
    if !can_read_details {
        return "busy".to_string();
    }
    match status.trim().to_ascii_lowercase().as_str() {
        "tentative" => "tentative".to_string(),
        _ => "busy".to_string(),
    }
}

struct DelegateFreeBusyProjection {
    id: Uuid,
    message_kind: String,
    subject: String,
    body_text: String,
    starts_at: Option<String>,
    ends_at: Option<String>,
    busy_status: Option<String>,
    payload_json: serde_json::Value,
}

fn delegate_freebusy_projections(
    principal_account_id: Uuid,
    owner_account_id: Uuid,
    delegate: Option<&DelegateAccessObject>,
    free_busy: Vec<FreeBusyBlock>,
) -> Result<Vec<DelegateFreeBusyProjection>> {
    let mut projected = Vec::new();
    if let Some(delegate) = delegate {
        projected.push(DelegateFreeBusyProjection {
            id: stable_delegate_freebusy_id(&[
                "delegate",
                &principal_account_id.to_string(),
                &owner_account_id.to_string(),
            ]),
            message_kind: "delegate".to_string(),
            subject: format!("Delegate access for {}", delegate.owner_email),
            body_text: format!(
                "calendarRead={}; calendarWrite={}; meetingObjects={}; sendOnBehalf={}",
                delegate.can_open_calendar,
                delegate.can_create_or_update_calendar_items,
                delegate.can_receive_meeting_objects,
                delegate.can_send_on_behalf
            ),
            starts_at: None,
            ends_at: None,
            busy_status: None,
            payload_json: serde_json::to_value(delegate)?,
        });
    }
    for block in free_busy {
        projected.push(DelegateFreeBusyProjection {
            id: stable_delegate_freebusy_id(&[
                "freebusy",
                &principal_account_id.to_string(),
                &owner_account_id.to_string(),
                &block.start,
                &block.end,
                &block.status,
            ]),
            message_kind: "freebusy".to_string(),
            subject: format!("{}: {}", block.owner_email, block.status),
            body_text: format!("{} from {} to {}", block.status, block.start, block.end),
            starts_at: Some(block.start),
            ends_at: Some(block.end),
            busy_status: Some(block.status),
            payload_json: serde_json::json!({
                "ownerAccountId": owner_account_id,
                "ownerEmail": block.owner_email,
            }),
        });
    }
    Ok(projected)
}

pub(super) fn delegate_freebusy_message_objects(
    principal_account_id: Uuid,
    owner_account_id: Uuid,
    delegate: Option<&DelegateAccessObject>,
    free_busy: Vec<FreeBusyBlock>,
) -> Result<Vec<DelegateFreeBusyMessageObject>> {
    let owner_email = delegate
        .map(|delegate| delegate.owner_email.clone())
        .or_else(|| free_busy.first().map(|block| block.owner_email.clone()))
        .unwrap_or_default();
    delegate_freebusy_projections(principal_account_id, owner_account_id, delegate, free_busy)?
        .into_iter()
        .map(|message| {
            let updated_at = delegate_freebusy_projection_updated_at(&message);
            Ok(DelegateFreeBusyMessageObject {
                id: message.id,
                account_id: principal_account_id,
                owner_account_id,
                owner_email: owner_email.clone(),
                message_kind: message.message_kind,
                subject: message.subject,
                body_text: message.body_text,
                starts_at: message.starts_at,
                ends_at: message.ends_at,
                busy_status: message.busy_status,
                payload_json: message.payload_json.to_string(),
                updated_at,
            })
        })
        .collect()
}

fn delegate_freebusy_projection_updated_at(message: &DelegateFreeBusyProjection) -> String {
    message
        .ends_at
        .as_ref()
        .or(message.starts_at.as_ref())
        .cloned()
        .unwrap_or_else(|| "1970-01-01T00:00:00Z".to_string())
}

fn stable_delegate_freebusy_id(parts: &[&str]) -> Uuid {
    let mut hasher = Sha256::new();
    for part in parts {
        hasher.update(part.as_bytes());
        hasher.update([0]);
    }
    let digest = hasher.finalize();
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Uuid::from_bytes(bytes)
}

#[cfg(test)]
mod free_busy_tests {
    use super::*;

    #[test]
    fn free_busy_rows_merge_adjacent_matching_states() {
        let owner_account_id = Uuid::parse_str("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa").unwrap();
        let blocks = merge_free_busy_rows(
            vec![
                crate::FreeBusyEventRow {
                    starts_at: "2026-05-26T08:00:00Z".to_string(),
                    ends_at: "2026-05-26T09:00:00Z".to_string(),
                    status: "confirmed".to_string(),
                },
                crate::FreeBusyEventRow {
                    starts_at: "2026-05-26T09:00:00Z".to_string(),
                    ends_at: "2026-05-26T10:00:00Z".to_string(),
                    status: "confirmed".to_string(),
                },
                crate::FreeBusyEventRow {
                    starts_at: "2026-05-26T10:30:00Z".to_string(),
                    ends_at: "2026-05-26T11:00:00Z".to_string(),
                    status: "tentative".to_string(),
                },
            ],
            owner_account_id,
            "owner@example.test".to_string(),
            true,
        );

        assert_eq!(blocks.len(), 2);
        assert_eq!(blocks[0].start, "2026-05-26T08:00:00Z");
        assert_eq!(blocks[0].end, "2026-05-26T10:00:00Z");
        assert_eq!(blocks[0].status, "busy");
        assert_eq!(blocks[1].status, "tentative");
    }

    #[test]
    fn free_busy_without_calendar_access_hides_tentative_detail() {
        let owner_account_id = Uuid::parse_str("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa").unwrap();
        let blocks = merge_free_busy_rows(
            vec![crate::FreeBusyEventRow {
                starts_at: "2026-05-26T08:00:00Z".to_string(),
                ends_at: "2026-05-26T09:00:00Z".to_string(),
                status: "tentative".to_string(),
            }],
            owner_account_id,
            "owner@example.test".to_string(),
            false,
        );

        assert_eq!(blocks[0].status, "busy");
    }

    #[test]
    fn free_busy_cancelled_rows_stay_free_without_calendar_access() {
        let owner_account_id = Uuid::parse_str("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa").unwrap();
        let blocks = merge_free_busy_rows(
            vec![crate::FreeBusyEventRow {
                starts_at: "2026-05-26T08:00:00Z".to_string(),
                ends_at: "2026-05-26T09:00:00Z".to_string(),
                status: "cancelled".to_string(),
            }],
            owner_account_id,
            "owner@example.test".to_string(),
            false,
        );

        assert!(blocks.is_empty());
    }

    #[test]
    fn delegate_freebusy_projection_does_not_create_empty_placeholder() {
        let principal_account_id = Uuid::parse_str("11111111-1111-4111-8111-111111111111").unwrap();
        let owner_account_id = Uuid::parse_str("22222222-2222-4222-8222-222222222222").unwrap();

        let projected =
            delegate_freebusy_projections(principal_account_id, owner_account_id, None, vec![])
                .unwrap();

        assert!(projected.is_empty());
    }

    #[test]
    fn delegate_freebusy_projection_uses_only_canonical_delegate_and_blocks() {
        let principal_account_id = Uuid::parse_str("11111111-1111-4111-8111-111111111111").unwrap();
        let owner_account_id = Uuid::parse_str("22222222-2222-4222-8222-222222222222").unwrap();
        let delegate = DelegateAccessObject {
            owner_account_id,
            owner_email: "owner@example.test".to_string(),
            grantee_account_id: principal_account_id,
            grantee_email: "delegate@example.test".to_string(),
            can_view_free_busy: true,
            can_open_calendar: true,
            can_create_or_update_calendar_items: true,
            can_delete_calendar_items: false,
            can_receive_meeting_objects: true,
            can_send_on_behalf: true,
        };
        let free_busy = vec![FreeBusyBlock {
            owner_account_id,
            owner_email: "owner@example.test".to_string(),
            start: "2026-05-30T08:00:00Z".to_string(),
            end: "2026-05-30T09:00:00Z".to_string(),
            status: "busy".to_string(),
        }];

        let projected = delegate_freebusy_projections(
            principal_account_id,
            owner_account_id,
            Some(&delegate),
            free_busy,
        )
        .unwrap();

        assert_eq!(projected.len(), 2);
        assert_eq!(projected[0].message_kind, "delegate");
        assert_eq!(projected[1].message_kind, "freebusy");
    }

    #[test]
    pub(super) fn delegate_freebusy_message_objects_use_interval_commit_time_without_store_state() {
        let principal_account_id = Uuid::parse_str("11111111-1111-4111-8111-111111111111").unwrap();
        let owner_account_id = Uuid::parse_str("22222222-2222-4222-8222-222222222222").unwrap();
        let messages = delegate_freebusy_message_objects(
            principal_account_id,
            owner_account_id,
            None,
            vec![FreeBusyBlock {
                owner_account_id,
                owner_email: "owner@example.test".to_string(),
                start: "2026-05-30T08:00:00Z".to_string(),
                end: "2026-05-30T09:00:00Z".to_string(),
                status: "busy".to_string(),
            }],
        )
        .unwrap();

        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].updated_at, "2026-05-30T09:00:00Z");
        assert_eq!(
            messages[0].payload_json,
            r#"{"ownerAccountId":"22222222-2222-4222-8222-222222222222","ownerEmail":"owner@example.test"}"#
        );
    }
}
