---
type: Rust Function
title: validate_append_attachments
resource: crates/lpe-imap/src/append.rs#L238-L258
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-magika/src/mime/collect_mime_attachment_parts
  called_by:
  - functions/crates/lpe-imap/src/append/Session/handle_append
---

# Signature

`fn validate_append_attachments<D: Detector>(validator: &Validator<D>, bytes: &[u8]) -> Result<()>`

# Calls

- [collect_mime_attachment_parts](../../../../../functions/crates/lpe-magika/src/mime/collect_mime_attachment_parts.md)

# Called by

- [handle_append](../../../../../functions/crates/lpe-imap/src/append/Session/handle_append.md)