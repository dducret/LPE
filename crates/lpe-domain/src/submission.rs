use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::encoding::base64_bytes;

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
    pub accepted: bool,
    pub delivered_mailboxes: Vec<String>,
    pub detail: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SmtpSubmissionAuthRequest {
    pub login: String,
    pub password: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SmtpSubmissionAuthResponse {
    pub accepted: bool,
    pub account_id: Option<Uuid>,
    pub account_email: Option<String>,
    pub account_display_name: Option<String>,
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
    pub accepted: bool,
    pub trace_id: String,
    pub detail: Option<String>,
}
