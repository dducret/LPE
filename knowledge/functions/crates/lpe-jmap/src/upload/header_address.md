---
type: Rust Function
title: header_address
resource: crates/lpe-jmap/src/upload.rs#L204-L206
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-domain/src/mail_format/format_mailbox_address
  called_by:
  - functions/crates/lpe-jmap/src/upload/message_rfc822_bytes
  - functions/crates/lpe-jmap/src/upload/push_address_list
---

# Signature

`fn header_address(address: &str, display_name: Option<&str>) -> String`

# Calls

- [format_mailbox_address](../../../../../functions/crates/lpe-domain/src/mail_format/format_mailbox_address.md)

# Called by

- [message_rfc822_bytes](../../../../../functions/crates/lpe-jmap/src/upload/message_rfc822_bytes.md)
- [push_address_list](../../../../../functions/crates/lpe-jmap/src/upload/push_address_list.md)