---
type: Rust Module
title: integration
resource: crates/lpe-admin-api/src/integration.rs#L1-L738
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/crate-bad-request-error-ha-allows-active-work-ha-current-role-http-internal-error-integration-shared-secret-observability-types-apiresult
  - external/axum-extract-state-http-headermap-statuscode-json
  - external/lpe-domain-current-unix-timestamp-bridgeautherror-inbounddeliveryrequest-inbounddeliveryresponse-recipientverificationrequest-recipientverificationresponse-signedintegrationheaders-smtpsubmissionauthrequest-smtpsubmissionauthresponse-smtpsubmissionrequest-smtpsubmissionresponse-default-max-skew-seconds-integration-key-header-integration-nonce-header-integration-signature-header-integration-timestamp-header
  - external/lpe-magika-collect-mime-attachment-parts-expectedkind-ingresscontext-policydecision-validationrequest-validator
  - external/lpe-mail-auth-authenticate-plain-credentials-accountprincipal
  - external/lpe-storage-auditentryinput-storage-submissionaccountidentity-submitmessageinput-submittedrecipientinput
  - external/tracing-info
  - external/super-classify-submission-account-identity-error-parse-required-submission-from-parse-smtp-submission-sender-require-integration-smtpsubmissionerror
  - external/axum-http-headermap
  - external/lpe-domain-signedintegrationheaders-smtpsubmissionauthrequest-integration-key-header-integration-nonce-header-integration-signature-header-integration-timestamp-header
  - external/std-sync-mutex
  member_of:
  - packages/crates/lpe-admin-api
---

# Contains

- [SmtpSubmissionError](../../../../classes/crates/lpe-admin-api/src/integration/SmtpSubmissionError.md)
- [invalid](../../../../functions/crates/lpe-admin-api/src/integration/SmtpSubmissionError/invalid.md)
- [forbidden](../../../../functions/crates/lpe-admin-api/src/integration/SmtpSubmissionError/forbidden.md)
- [temporary](../../../../functions/crates/lpe-admin-api/src/integration/SmtpSubmissionError/temporary.md)
- [into_http_error](../../../../functions/crates/lpe-admin-api/src/integration/SmtpSubmissionError/into_http_error.md)
- [deliver_inbound_message](../../../../functions/crates/lpe-admin-api/src/integration/deliver_inbound_message.md)
- [verify_lpe_ct_recipient](../../../../functions/crates/lpe-admin-api/src/integration/verify_lpe_ct_recipient.md)
- [authenticate_smtp_submission](../../../../functions/crates/lpe-admin-api/src/integration/authenticate_smtp_submission.md)
- [accept_smtp_submission](../../../../functions/crates/lpe-admin-api/src/integration/accept_smtp_submission.md)
- [build_smtp_submission_input](../../../../functions/crates/lpe-admin-api/src/integration/build_smtp_submission_input.md)
- [load_authenticated_submission_principal](../../../../functions/crates/lpe-admin-api/src/integration/load_authenticated_submission_principal.md)
- [classify_submission_account_identity_error](../../../../functions/crates/lpe-admin-api/src/integration/classify_submission_account_identity_error.md)
- [parse_required_submission_from](../../../../functions/crates/lpe-admin-api/src/integration/parse_required_submission_from.md)
- [build_smtp_submission_input_for_owner](../../../../functions/crates/lpe-admin-api/src/integration/build_smtp_submission_input_for_owner.md)
- [parse_smtp_submission_sender](../../../../functions/crates/lpe-admin-api/src/integration/parse_smtp_submission_sender.md)
- [merge_smtp_bcc_recipients](../../../../functions/crates/lpe-admin-api/src/integration/merge_smtp_bcc_recipients.md)
- [validate_smtp_submission_attachments](../../../../functions/crates/lpe-admin-api/src/integration/validate_smtp_submission_attachments.md)
- [classify_submission_storage_error](../../../../functions/crates/lpe-admin-api/src/integration/classify_submission_storage_error.md)
- [require_integration](../../../../functions/crates/lpe-admin-api/src/integration/require_integration.md)
- [required_header](../../../../functions/crates/lpe-admin-api/src/integration/required_header.md)
- [ensure_not_replayed](../../../../functions/crates/lpe-admin-api/src/integration/ensure_not_replayed.md)
- [integration_auth_error](../../../../functions/crates/lpe-admin-api/src/integration/integration_auth_error.md)
- [smtp_submission_requires_exactly_one_from_mailbox](../../../../functions/crates/lpe-admin-api/src/integration/smtp_submission_requires_exactly_one_from_mailbox.md)
- [smtp_submission_sender_rejects_multiple_sender_mailboxes](../../../../functions/crates/lpe-admin-api/src/integration/smtp_submission_sender_rejects_multiple_sender_mailboxes.md)
- [smtp_submission_sender_rejects_unrelated_sender_identity](../../../../functions/crates/lpe-admin-api/src/integration/smtp_submission_sender_rejects_unrelated_sender_identity.md)
- [submission_account_identity_errors_distinguish_missing_account_from_temporary_failures](../../../../functions/crates/lpe-admin-api/src/integration/submission_account_identity_errors_distinguish_missing_account_from_temporary_failures.md)
- [integration_requests_require_signed_headers_and_reject_replay](../../../../functions/crates/lpe-admin-api/src/integration/integration_requests_require_signed_headers_and_reject_replay.md)

# Imports

- `crate::{
    bad_request_error, ha_allows_active_work, ha_current_role, http::internal_error,
    integration_shared_secret, observability, types::ApiResult,
}`
- `axum::{
    extract::State,
    http::{HeaderMap, StatusCode},
    Json,
}`
- `lpe_domain::{
    current_unix_timestamp, BridgeAuthError, InboundDeliveryRequest, InboundDeliveryResponse,
    RecipientVerificationRequest, RecipientVerificationResponse, SignedIntegrationHeaders,
    SmtpSubmissionAuthRequest, SmtpSubmissionAuthResponse, SmtpSubmissionRequest,
    SmtpSubmissionResponse, DEFAULT_MAX_SKEW_SECONDS, INTEGRATION_KEY_HEADER,
    INTEGRATION_NONCE_HEADER, INTEGRATION_SIGNATURE_HEADER, INTEGRATION_TIMESTAMP_HEADER,
}`
- `lpe_magika::{
    collect_mime_attachment_parts, ExpectedKind, IngressContext, PolicyDecision, ValidationRequest,
    Validator,
}`
- `lpe_mail_auth::{authenticate_plain_credentials, AccountPrincipal}`
- `lpe_storage::{
    AuditEntryInput, Storage, SubmissionAccountIdentity, SubmitMessageInput,
    SubmittedRecipientInput,
}`
- `tracing::info`
- `super::{
        classify_submission_account_identity_error, parse_required_submission_from,
        parse_smtp_submission_sender, require_integration, SmtpSubmissionError,
    }`
- `axum::http::HeaderMap`
- `lpe_domain::{
        SignedIntegrationHeaders, SmtpSubmissionAuthRequest, INTEGRATION_KEY_HEADER,
        INTEGRATION_NONCE_HEADER, INTEGRATION_SIGNATURE_HEADER, INTEGRATION_TIMESTAMP_HEADER,
    }`
- `std::sync::Mutex`

# Member of

- [lpe-admin-api](../../../../packages/crates/lpe-admin-api.md)