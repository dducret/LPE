---
type: Rust Function
title: validate_mime_attachments
resource: crates/lpe-activesync/src/service/mime_validation.rs#L6-L8
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/service/mime_validation/validate_mime_attachments_with_validator
  called_by:
  - functions/crates/lpe-activesync/src/service/submission/ActiveSyncService/handle_send_mail
  - functions/crates/lpe-activesync/src/service/submission/ActiveSyncService/handle_smart_compose
---

# Signature

`pub(super) fn validate_mime_attachments(bytes: &[u8]) -> Result<()>`

# Calls

- [validate_mime_attachments_with_validator](../../../../../../functions/crates/lpe-activesync/src/service/mime_validation/validate_mime_attachments_with_validator.md)

# Called by

- [handle_send_mail](../../../../../../functions/crates/lpe-activesync/src/service/submission/ActiveSyncService/handle_send_mail.md)
- [handle_smart_compose](../../../../../../functions/crates/lpe-activesync/src/service/submission/ActiveSyncService/handle_smart_compose.md)