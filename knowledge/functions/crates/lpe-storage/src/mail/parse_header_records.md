---
type: Rust Function
title: parse_header_records
resource: crates/lpe-storage/src/mail.rs#L126-L167
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-storage/src/mail/parse_message_date_header
  - functions/crates/lpe-storage/src/shared/Storage/replace_message_headers_in_tx
---

# Signature

`pub fn parse_header_records(raw_message: &[u8]) -> Vec<ParsedRfc822Header>`

# Calls

- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [parse_message_date_header](../../../../../functions/crates/lpe-storage/src/mail/parse_message_date_header.md)
- [replace_message_headers_in_tx](../../../../../functions/crates/lpe-storage/src/shared/Storage/replace_message_headers_in_tx.md)