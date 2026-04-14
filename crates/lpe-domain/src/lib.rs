use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AccountId(pub Uuid);

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Account {
    pub id: AccountId,
    pub primary_email: String,
    pub display_name: String,
}

impl Account {
    pub fn new(primary_email: impl Into<String>, display_name: impl Into<String>) -> Self {
        Self {
            id: AccountId(Uuid::new_v4()),
            primary_email: primary_email.into(),
            display_name: display_name.into(),
        }
    }
}

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
