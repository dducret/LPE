---
type: Rust Function
title: normalize_mime_body
resource: crates/lpe-domain/src/mail_format.rs#L51-L58
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-jmap/src/upload/message_rfc822_bytes
  - functions/crates/lpe-jmap/src/upload/push_mime_part
---

# Signature

`pub fn normalize_mime_body(value: &str) -> String`

# Called by

- [message_rfc822_bytes](../../../../../functions/crates/lpe-jmap/src/upload/message_rfc822_bytes.md)
- [push_mime_part](../../../../../functions/crates/lpe-jmap/src/upload/push_mime_part.md)