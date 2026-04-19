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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum TransportDeliveryStatus {
    Queued,
    Relayed,
    Deferred,
    Quarantined,
    Failed,
}

impl TransportDeliveryStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Relayed => "relayed",
            Self::Deferred => "deferred",
            Self::Quarantined => "quarantined",
            Self::Failed => "failed",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TransportRecipient {
    pub address: String,
    pub display_name: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OutboundMessageHandoffRequest {
    pub queue_id: Uuid,
    pub message_id: Uuid,
    pub account_id: Uuid,
    pub from_address: String,
    pub from_display: Option<String>,
    pub to: Vec<TransportRecipient>,
    pub cc: Vec<TransportRecipient>,
    pub bcc: Vec<TransportRecipient>,
    pub subject: String,
    pub body_text: String,
    pub body_html_sanitized: Option<String>,
    pub internet_message_id: Option<String>,
}

impl OutboundMessageHandoffRequest {
    pub fn envelope_recipients(&self) -> Vec<String> {
        self.to
            .iter()
            .chain(self.cc.iter())
            .chain(self.bcc.iter())
            .map(|recipient| recipient.address.clone())
            .collect()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OutboundMessageHandoffResponse {
    pub queue_id: Uuid,
    pub status: TransportDeliveryStatus,
    pub trace_id: String,
    pub detail: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct InboundDeliveryRequest {
    pub trace_id: String,
    pub peer: String,
    pub helo: String,
    pub mail_from: String,
    pub rcpt_to: Vec<String>,
    pub subject: String,
    pub body_text: String,
    pub internet_message_id: Option<String>,
    pub raw_message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct InboundDeliveryResponse {
    pub trace_id: String,
    pub status: TransportDeliveryStatus,
    pub accepted_recipients: Vec<String>,
    pub rejected_recipients: Vec<String>,
    pub stored_message_ids: Vec<Uuid>,
    pub detail: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::{OutboundMessageHandoffRequest, TransportDeliveryStatus, TransportRecipient};
    use uuid::Uuid;

    #[test]
    fn transport_delivery_status_serializes_as_lowercase() {
        let value = serde_json::to_string(&TransportDeliveryStatus::Deferred).unwrap();
        assert_eq!(value, "\"deferred\"");
    }

    #[test]
    fn outbound_envelope_recipients_include_bcc() {
        let request = OutboundMessageHandoffRequest {
            queue_id: Uuid::nil(),
            message_id: Uuid::nil(),
            account_id: Uuid::nil(),
            from_address: "sender@example.test".to_string(),
            from_display: None,
            to: vec![TransportRecipient {
                address: "to@example.test".to_string(),
                display_name: None,
            }],
            cc: vec![TransportRecipient {
                address: "cc@example.test".to_string(),
                display_name: None,
            }],
            bcc: vec![TransportRecipient {
                address: "bcc@example.test".to_string(),
                display_name: None,
            }],
            subject: "subject".to_string(),
            body_text: "body".to_string(),
            body_html_sanitized: None,
            internet_message_id: None,
        };

        assert_eq!(
            request.envelope_recipients(),
            vec![
                "to@example.test".to_string(),
                "cc@example.test".to_string(),
                "bcc@example.test".to_string()
            ]
        );
    }
}
