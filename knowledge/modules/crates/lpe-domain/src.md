---
type: Rust Module
title: src
resource: crates/lpe-domain/src/lib.rs#L1-L44
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/pub-use-crate-account-account-accountid
  - external/pub-use-crate-bridge-auth-current-unix-timestamp-bridgeautherror-signedintegrationheaders-default-max-skew-seconds-integration-key-header-integration-nonce-header-integration-signature-header-integration-timestamp-header
  - external/pub-use-crate-civil-time-civil-from-days-current-windows-filetime-days-from-civil-month-abbrev-unix-seconds-from-windows-filetime-utc-from-unix-seconds-weekday-abbrev-from-unix-days-windows-filetime-from-signed-unix-seconds-windows-filetime-from-unix-seconds-utcdatetime-windows-filetime-ticks-per-second-windows-unix-epoch-offset-seconds
  - external/pub-use-crate-document-accessscope-documentannotation-documentchunk-documentkind-documentprojection
  - external/pub-use-crate-mailbox-name-mailboxcanonicalkey-mailboxdisplayname-mailboxnameerror-mailboxnamepolicy-mailboxpath-mailboxsegment-mailbox-hierarchy-delimiter
  - external/pub-use-crate-submission-inbounddeliveryrequest-inbounddeliveryresponse-recipientverificationrequest-recipientverificationresponse-smtpsubmissionauthrequest-smtpsubmissionauthresponse-smtpsubmissionrequest-smtpsubmissionresponse
  - external/pub-use-crate-transport-outboundmessagehandoffrequest-outboundmessagehandoffresponse-transportdeliverystatus-transportdsnreport-transportrecipient-transportretryadvice-transportroutedecision-transporttechnicalstatus-transportthrottlestatus
  member_of:
  - packages/crates/lpe-domain
---

# Imports

- `pub use crate::account::{Account, AccountId}`
- `pub use crate::bridge_auth::{
    current_unix_timestamp, BridgeAuthError, SignedIntegrationHeaders, DEFAULT_MAX_SKEW_SECONDS,
    INTEGRATION_KEY_HEADER, INTEGRATION_NONCE_HEADER, INTEGRATION_SIGNATURE_HEADER,
    INTEGRATION_TIMESTAMP_HEADER,
}`
- `pub use crate::civil_time::{
    civil_from_days, current_windows_filetime, days_from_civil, month_abbrev,
    unix_seconds_from_windows_filetime, utc_from_unix_seconds, weekday_abbrev_from_unix_days,
    windows_filetime_from_signed_unix_seconds, windows_filetime_from_unix_seconds, UtcDateTime,
    WINDOWS_FILETIME_TICKS_PER_SECOND, WINDOWS_UNIX_EPOCH_OFFSET_SECONDS,
}`
- `pub use crate::document::{
    AccessScope, DocumentAnnotation, DocumentChunk, DocumentKind, DocumentProjection,
}`
- `pub use crate::mailbox_name::{
    MailboxCanonicalKey, MailboxDisplayName, MailboxNameError, MailboxNamePolicy, MailboxPath,
    MailboxSegment, MAILBOX_HIERARCHY_DELIMITER,
}`
- `pub use crate::submission::{
    InboundDeliveryRequest, InboundDeliveryResponse, RecipientVerificationRequest,
    RecipientVerificationResponse, SmtpSubmissionAuthRequest, SmtpSubmissionAuthResponse,
    SmtpSubmissionRequest, SmtpSubmissionResponse,
}`
- `pub use crate::transport::{
    OutboundMessageHandoffRequest, OutboundMessageHandoffResponse, TransportDeliveryStatus,
    TransportDsnReport, TransportRecipient, TransportRetryAdvice, TransportRouteDecision,
    TransportTechnicalStatus, TransportThrottleStatus,
}`

# Member of

- [lpe-domain](../../../packages/crates/lpe-domain.md)