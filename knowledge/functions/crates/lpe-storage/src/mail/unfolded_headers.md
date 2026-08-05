---
type: Rust Function
title: unfolded_headers
resource: crates/lpe-storage/src/mail.rs#L220-L245
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-storage/src/mail/parse_header_recipients
---

# Signature

`fn unfolded_headers(raw_message: &[u8]) -> Vec<String>`

# Calls

- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [parse_header_recipients](../../../../../functions/crates/lpe-storage/src/mail/parse_header_recipients.md)