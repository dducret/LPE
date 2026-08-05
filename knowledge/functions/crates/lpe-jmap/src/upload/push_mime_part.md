---
type: Rust Function
title: push_mime_part
resource: crates/lpe-jmap/src/upload.rs#L170-L183
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/upload/push_header
  - functions/crates/lpe-domain/src/mail_format/normalize_mime_body
  called_by:
  - functions/crates/lpe-jmap/src/upload/message_rfc822_bytes
---

# Signature

`fn push_mime_part(message: &mut String, boundary: &str, media_type: &str, body: &str)`

# Calls

- [push_header](../../../../../functions/crates/lpe-jmap/src/upload/push_header.md)
- [normalize_mime_body](../../../../../functions/crates/lpe-domain/src/mail_format/normalize_mime_body.md)

# Called by

- [message_rfc822_bytes](../../../../../functions/crates/lpe-jmap/src/upload/message_rfc822_bytes.md)