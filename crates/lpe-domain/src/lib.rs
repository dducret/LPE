use base64::engine::general_purpose::STANDARD as BASE64;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

mod base64_bytes {
    use super::BASE64;
    use base64::Engine;
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(value: &[u8], serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&BASE64.encode(value))
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let encoded = String::deserialize(deserializer)?;
        BASE64.decode(encoded).map_err(serde::de::Error::custom)
    }
}

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
    Bounced,
    Failed,
}

impl TransportDeliveryStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Relayed => "relayed",
            Self::Deferred => "deferred",
            Self::Quarantined => "quarantined",
            Self::Bounced => "bounced",
            Self::Failed => "failed",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TransportRetryAdvice {
    pub retry_after_seconds: u32,
    pub policy: String,
    pub reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TransportDsnReport {
    pub action: String,
    pub status: String,
    pub diagnostic_code: Option<String>,
    pub remote_mta: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TransportTechnicalStatus {
    pub phase: String,
    pub smtp_code: Option<u16>,
    pub enhanced_code: Option<String>,
    pub remote_host: Option<String>,
    pub detail: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TransportRouteDecision {
    pub rule_id: Option<String>,
    pub relay_target: Option<String>,
    pub queue: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TransportThrottleStatus {
    pub scope: String,
    pub key: String,
    pub limit: u32,
    pub window_seconds: u32,
    pub retry_after_seconds: u32,
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
    pub attempt_count: u32,
    pub last_attempt_error: Option<String>,
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
    pub remote_message_ref: Option<String>,
    pub retry: Option<TransportRetryAdvice>,
    pub dsn: Option<TransportDsnReport>,
    pub technical: Option<TransportTechnicalStatus>,
    pub route: Option<TransportRouteDecision>,
    pub throttle: Option<TransportThrottleStatus>,
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
    #[serde(with = "base64_bytes")]
    pub raw_message: Vec<u8>,
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SmtpSubmissionAuthRequest {
    pub username: String,
    pub password: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SmtpSubmissionAuthResponse {
    pub tenant_id: String,
    pub account_id: Uuid,
    pub email: String,
    pub display_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SmtpSubmissionRequest {
    pub trace_id: String,
    pub helo: String,
    pub peer: String,
    pub account_id: Uuid,
    pub account_email: String,
    pub account_display_name: String,
    pub mail_from: String,
    pub rcpt_to: Vec<String>,
    #[serde(with = "base64_bytes")]
    pub raw_message: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SmtpSubmissionResponse {
    pub trace_id: String,
    pub message_id: Uuid,
    pub outbound_queue_id: Uuid,
    pub delivery_status: String,
}

#[cfg(test)]
mod tests {
    use super::{
        OutboundMessageHandoffRequest, OutboundMessageHandoffResponse, SmtpSubmissionRequest,
        TransportDeliveryStatus, TransportDsnReport, TransportRecipient, TransportRetryAdvice,
        TransportRouteDecision, TransportTechnicalStatus, TransportThrottleStatus,
    };
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
            attempt_count: 0,
            last_attempt_error: None,
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

    #[test]
    fn outbound_handoff_response_carries_structured_transport_details() {
        let response = OutboundMessageHandoffResponse {
            queue_id: Uuid::nil(),
            status: TransportDeliveryStatus::Deferred,
            trace_id: "trace-1".to_string(),
            detail: Some("rate limit reached".to_string()),
            remote_message_ref: Some("remote-42".to_string()),
            retry: Some(TransportRetryAdvice {
                retry_after_seconds: 120,
                policy: "throttle".to_string(),
                reason: Some("tenant quota".to_string()),
            }),
            dsn: Some(TransportDsnReport {
                action: "delayed".to_string(),
                status: "4.7.1".to_string(),
                diagnostic_code: Some("smtp; 451 4.7.1 throttled".to_string()),
                remote_mta: Some("mx1.example.test".to_string()),
            }),
            technical: Some(TransportTechnicalStatus {
                phase: "rcpt-to".to_string(),
                smtp_code: Some(451),
                enhanced_code: Some("4.7.1".to_string()),
                remote_host: Some("mx1.example.test".to_string()),
                detail: Some("recipient domain throttled".to_string()),
            }),
            route: Some(TransportRouteDecision {
                rule_id: Some("domain-example".to_string()),
                relay_target: Some("smtp://mx1.example.test:25".to_string()),
                queue: "deferred".to_string(),
            }),
            throttle: Some(TransportThrottleStatus {
                scope: "recipient-domain".to_string(),
                key: "example.test".to_string(),
                limit: 20,
                window_seconds: 60,
                retry_after_seconds: 120,
            }),
        };

        let json = serde_json::to_value(&response).unwrap();
        assert_eq!(json["status"], "deferred");
        assert_eq!(json["retry"]["retry_after_seconds"], 120);
        assert_eq!(json["dsn"]["status"], "4.7.1");
        assert_eq!(json["route"]["queue"], "deferred");
        assert_eq!(json["throttle"]["scope"], "recipient-domain");
    }

    #[test]
    fn smtp_submission_request_serializes_raw_message_as_base64() {
        let request = SmtpSubmissionRequest {
            trace_id: "trace-1".to_string(),
            helo: "client.example.test".to_string(),
            peer: "203.0.113.10:53544".to_string(),
            account_id: Uuid::nil(),
            account_email: "alice@example.test".to_string(),
            account_display_name: "Alice".to_string(),
            mail_from: "alice@example.test".to_string(),
            rcpt_to: vec!["bob@example.test".to_string()],
            raw_message: b"Subject: hi\r\n\r\nbody".to_vec(),
        };

        let json = serde_json::to_value(&request).unwrap();
        assert_eq!(json["raw_message"], "U3ViamVjdDogaGkNCg0KYm9keQ==");
    }
}
