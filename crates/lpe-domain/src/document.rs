use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::account::AccountId;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum DocumentKind {
    MailMessage,
    CalendarEvent,
    Contact,
    Attachment,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AccessScope {
    pub tenant_id: String,
    pub owner_account_id: AccountId,
    pub acl_fingerprint: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DocumentProjection {
    pub id: Uuid,
    pub source_object_id: Uuid,
    pub kind: DocumentKind,
    pub title: String,
    pub preview: String,
    pub body_text: String,
    pub language: Option<String>,
    pub participants: Vec<String>,
    pub content_hash: String,
    pub scope: AccessScope,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DocumentChunk {
    pub id: Uuid,
    pub document_id: Uuid,
    pub ordinal: i32,
    pub chunk_text: String,
    pub token_estimate: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DocumentAnnotation {
    pub id: Uuid,
    pub document_id: Uuid,
    pub annotation_type: String,
    pub payload_json: String,
    pub model_name: Option<String>,
}
