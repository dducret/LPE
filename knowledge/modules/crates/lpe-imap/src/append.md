---
type: Rust Module
title: append
resource: crates/lpe-imap/src/append.rs#L1-L285
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/anyhow-anyhow-bail-result
  - external/lpe-magika-collect-mime-attachment-parts-detector-expectedkind-ingresscontext-policydecision-validationrequest-validator
  - external/lpe-storage-mail-parse-rfc822-message-auditentryinput-jmapimportedemailinput-jmapmailbox-submitmessageinput
  - external/tokio-io-asyncreadext-asyncwriteext-bufreader
  - external/uuid-uuid
  - external/crate-parse-parse-literal-size-tokenize-session
  member_of:
  - packages/crates/lpe-imap
---

# Contains

- [handle_append](../../../../functions/crates/lpe-imap/src/append/Session/handle_append.md)
- [resolve_append_mailbox](../../../../functions/crates/lpe-imap/src/append/Session/resolve_append_mailbox.md)
- [validate_append_attachments](../../../../functions/crates/lpe-imap/src/append/validate_append_attachments.md)
- [sent_append_ack_uid](../../../../functions/crates/lpe-imap/src/append/sent_append_ack_uid.md)
- [message_ids_match](../../../../functions/crates/lpe-imap/src/append/message_ids_match.md)
- [normalize_message_id](../../../../functions/crates/lpe-imap/src/append/normalize_message_id.md)

# Imports

- `anyhow::{anyhow, bail, Result}`
- `lpe_magika::{
    collect_mime_attachment_parts, Detector, ExpectedKind, IngressContext, PolicyDecision,
    ValidationRequest, Validator,
}`
- `lpe_storage::{
    mail::parse_rfc822_message, AuditEntryInput, JmapImportedEmailInput, JmapMailbox,
    SubmitMessageInput,
}`
- `tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader}`
- `uuid::Uuid`
- `crate::{
    parse::{parse_literal_size, tokenize},
    Session,
}`

# Member of

- [lpe-imap](../../../../packages/crates/lpe-imap.md)