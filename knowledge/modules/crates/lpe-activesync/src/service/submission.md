---
type: Rust Module
title: submission
resource: crates/lpe-activesync/src/service/submission.rs#L1-L348
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/anyhow-anyhow-bail-result
  - external/axum-http-headermap-response-response
  - external/lpe-storage-auditentryinput-submitmessageinput-submittedrecipientinput
  - external/uuid-uuid
  - external/crate-message-default-sender-parse-mime-message-protocol-activesynccommand-activesyncstatus-response-empty-response-is-message-rfc822-wbxml-response-store-activesyncstore-types-authenticatedprincipal-wbxml-decode-wbxml-encode-wbxml-wbxmlnode
  - external/super-command-status-response-validate-mime-attachments-activesyncservice
  member_of:
  - packages/crates/lpe-activesync
---

# Contains

- [handle_send_mail](../../../../../functions/crates/lpe-activesync/src/service/submission/ActiveSyncService/handle_send_mail.md)
- [handle_smart_compose](../../../../../functions/crates/lpe-activesync/src/service/submission/ActiveSyncService/handle_smart_compose.md)
- [resolve_source_message](../../../../../functions/crates/lpe-activesync/src/service/submission/ActiveSyncService/resolve_source_message.md)
- [load_message_attachment_uploads](../../../../../functions/crates/lpe-activesync/src/service/submission/ActiveSyncService/load_message_attachment_uploads.md)
- [reply_recipients](../../../../../functions/crates/lpe-activesync/src/service/submission/reply_recipients.md)
- [default_reply_subject](../../../../../functions/crates/lpe-activesync/src/service/submission/default_reply_subject.md)
- [merge_smart_body](../../../../../functions/crates/lpe-activesync/src/service/submission/merge_smart_body.md)

# Imports

- `anyhow::{anyhow, bail, Result}`
- `axum::{http::HeaderMap, response::Response}`
- `lpe_storage::{AuditEntryInput, SubmitMessageInput, SubmittedRecipientInput}`
- `uuid::Uuid`
- `crate::{
    message::{default_sender, parse_mime_message},
    protocol::{ActiveSyncCommand, ActiveSyncStatus},
    response::{empty_response, is_message_rfc822, wbxml_response},
    store::ActiveSyncStore,
    types::AuthenticatedPrincipal,
    wbxml::{decode_wbxml, encode_wbxml, WbxmlNode},
}`
- `super::{command_status_response, validate_mime_attachments, ActiveSyncService}`

# Member of

- [lpe-activesync](../../../../../packages/crates/lpe-activesync.md)