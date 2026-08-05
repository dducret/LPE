---
type: Rust Function
title: push_address_list
resource: crates/lpe-jmap/src/upload.rs#L192-L202
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/upload/header_address
  - functions/crates/lpe-jmap/src/upload/push_header
  called_by:
  - functions/crates/lpe-jmap/src/upload/message_rfc822_bytes
---

# Signature

`fn push_address_list(message: &mut String, name: &str, addresses: &[JmapEmailAddress])`

# Calls

- [header_address](../../../../../functions/crates/lpe-jmap/src/upload/header_address.md)
- [push_header](../../../../../functions/crates/lpe-jmap/src/upload/push_header.md)

# Called by

- [message_rfc822_bytes](../../../../../functions/crates/lpe-jmap/src/upload/message_rfc822_bytes.md)