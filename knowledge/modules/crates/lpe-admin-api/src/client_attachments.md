---
type: Rust Module
title: client_attachments
resource: crates/lpe-admin-api/src/client_attachments.rs#L1-L395
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/anyhow-bail
  - external/axum-body-body-extract-multipart-path-as-axumpath-query-state-http-header-headermap-headervalue-statuscode-response-response-json
  - external/lpe-magika-detector-expectedkind-ingresscontext-policydecision-validationrequest-validator
  - external/lpe-storage-activesyncattachment-attachmentuploadinput-auditentryinput-authenticatedaccount-mailboxaccountaccess-storage
  - external/serde-deserialize-serialize
  - external/std-path-path-as-filepath
  - external/uuid-uuid
  - external/crate-http-internal-error-require-account
  - external/super-attachment-content-response-authorize-attachment-mailbox-access-normalized-attachment-file-name-validate-client-attachment-with-validator
  - external/axum-http-statuscode
  - external/lpe-magika-detectionsource-detector-magikadetection-validator
  - external/lpe-storage-attachmentuploadinput-mailboxaccountaccess
  member_of:
  - packages/crates/lpe-admin-api
---

# Contains

- [MessageAttachmentQuery](../../../../classes/crates/lpe-admin-api/src/client_attachments/MessageAttachmentQuery.md)
- [ClientAttachmentUploadResponse](../../../../classes/crates/lpe-admin-api/src/client_attachments/ClientAttachmentUploadResponse.md)
- [upload_draft_attachment](../../../../functions/crates/lpe-admin-api/src/client_attachments/upload_draft_attachment.md)
- [download_message_attachment](../../../../functions/crates/lpe-admin-api/src/client_attachments/download_message_attachment.md)
- [require_attachment_mailbox_access](../../../../functions/crates/lpe-admin-api/src/client_attachments/require_attachment_mailbox_access.md)
- [authorize_attachment_mailbox_access](../../../../functions/crates/lpe-admin-api/src/client_attachments/authorize_attachment_mailbox_access.md)
- [read_multipart_attachment](../../../../functions/crates/lpe-admin-api/src/client_attachments/read_multipart_attachment.md)
- [validate_client_attachment_with_validator](../../../../functions/crates/lpe-admin-api/src/client_attachments/validate_client_attachment_with_validator.md)
- [normalized_attachment_file_name](../../../../functions/crates/lpe-admin-api/src/client_attachments/normalized_attachment_file_name.md)
- [client_attachment_upload_response](../../../../functions/crates/lpe-admin-api/src/client_attachments/client_attachment_upload_response.md)
- [attachment_content_response](../../../../functions/crates/lpe-admin-api/src/client_attachments/attachment_content_response.md)
- [FakeDetector](../../../../classes/crates/lpe-admin-api/src/client_attachments/FakeDetector.md)
- [detect](../../../../functions/crates/lpe-admin-api/src/client_attachments/FakeDetector/detector/detect.md)
- [attachment](../../../../functions/crates/lpe-admin-api/src/client_attachments/attachment.md)
- [client_attachment_upload_requires_magika_acceptance](../../../../functions/crates/lpe-admin-api/src/client_attachments/client_attachment_upload_requires_magika_acceptance.md)
- [client_attachment_upload_rejects_magika_mismatch](../../../../functions/crates/lpe-admin-api/src/client_attachments/client_attachment_upload_rejects_magika_mismatch.md)
- [attachment_download_uses_safe_inline_headers](../../../../functions/crates/lpe-admin-api/src/client_attachments/attachment_download_uses_safe_inline_headers.md)
- [delegated_attachment_upload_requires_canonical_write_access](../../../../functions/crates/lpe-admin-api/src/client_attachments/delegated_attachment_upload_requires_canonical_write_access.md)
- [delegated_attachment_download_allows_canonical_read_access](../../../../functions/crates/lpe-admin-api/src/client_attachments/delegated_attachment_download_allows_canonical_read_access.md)
- [attachment_access_rejects_a_mailbox_absent_from_canonical_grants](../../../../functions/crates/lpe-admin-api/src/client_attachments/attachment_access_rejects_a_mailbox_absent_from_canonical_grants.md)

# Imports

- `anyhow::bail`
- `axum::{
    body::Body,
    extract::{Multipart, Path as AxumPath, Query, State},
    http::{header, HeaderMap, HeaderValue, StatusCode},
    response::Response,
    Json,
}`
- `lpe_magika::{
    Detector, ExpectedKind, IngressContext, PolicyDecision, ValidationRequest, Validator,
}`
- `lpe_storage::{
    ActiveSyncAttachment, AttachmentUploadInput, AuditEntryInput, AuthenticatedAccount,
    MailboxAccountAccess, Storage,
}`
- `serde::{Deserialize, Serialize}`
- `std::path::Path as FilePath`
- `uuid::Uuid`
- `crate::{http::internal_error, require_account}`
- `super::{
        attachment_content_response, authorize_attachment_mailbox_access,
        normalized_attachment_file_name, validate_client_attachment_with_validator,
    }`
- `axum::http::StatusCode`
- `lpe_magika::{DetectionSource, Detector, MagikaDetection, Validator}`
- `lpe_storage::{AttachmentUploadInput, MailboxAccountAccess}`

# Member of

- [lpe-admin-api](../../../../packages/crates/lpe-admin-api.md)