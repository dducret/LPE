---
type: Rust Function
title: push_header
resource: crates/lpe-jmap/src/upload.rs#L185-L190
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-domain/src/mail_format/sanitize_header_value
  called_by:
  - functions/crates/lpe-jmap/src/upload/message_rfc822_bytes
  - functions/crates/lpe-jmap/src/upload/push_mime_part
  - functions/crates/lpe-jmap/src/upload/push_address_list
---

# Signature

`fn push_header(message: &mut String, name: &str, value: &str)`

# Calls

- [sanitize_header_value](../../../../../functions/crates/lpe-domain/src/mail_format/sanitize_header_value.md)

# Called by

- [message_rfc822_bytes](../../../../../functions/crates/lpe-jmap/src/upload/message_rfc822_bytes.md)
- [push_mime_part](../../../../../functions/crates/lpe-jmap/src/upload/push_mime_part.md)
- [push_address_list](../../../../../functions/crates/lpe-jmap/src/upload/push_address_list.md)