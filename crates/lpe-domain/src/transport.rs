use serde::{Deserialize, Serialize};
use uuid::Uuid;

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
    pub sender_address: Option<String>,
    pub sender_display: Option<String>,
    pub sender_authorization_kind: String,
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
