pub mod account;
pub mod bridge_auth;
pub mod civil_time;
pub mod crypto;
pub mod document;
mod encoding;
pub mod mail_format;
pub mod mailbox_name;
pub mod normalization;
pub mod submission;
pub mod transport;

pub use crate::account::{Account, AccountId};
pub use crate::bridge_auth::{
    current_unix_timestamp, BridgeAuthError, SignedIntegrationHeaders, DEFAULT_MAX_SKEW_SECONDS,
    INTEGRATION_KEY_HEADER, INTEGRATION_NONCE_HEADER, INTEGRATION_SIGNATURE_HEADER,
    INTEGRATION_TIMESTAMP_HEADER,
};
pub use crate::civil_time::{
    civil_from_days, days_from_civil, month_abbrev, utc_from_unix_seconds,
    weekday_abbrev_from_unix_days, UtcDateTime,
};
pub use crate::document::{
    AccessScope, DocumentAnnotation, DocumentChunk, DocumentKind, DocumentProjection,
};
pub use crate::mailbox_name::{
    MailboxCanonicalKey, MailboxDisplayName, MailboxNameError, MailboxNamePolicy, MailboxPath,
    MailboxSegment, MAILBOX_HIERARCHY_DELIMITER,
};
pub use crate::submission::{
    InboundDeliveryRequest, InboundDeliveryResponse, RecipientVerificationRequest,
    RecipientVerificationResponse, SmtpSubmissionAuthRequest, SmtpSubmissionAuthResponse,
    SmtpSubmissionRequest, SmtpSubmissionResponse,
};
pub use crate::transport::{
    OutboundMessageHandoffRequest, OutboundMessageHandoffResponse, TransportDeliveryStatus,
    TransportDsnReport, TransportRecipient, TransportRetryAdvice, TransportRouteDecision,
    TransportTechnicalStatus, TransportThrottleStatus,
};

#[cfg(test)]
mod tests;
