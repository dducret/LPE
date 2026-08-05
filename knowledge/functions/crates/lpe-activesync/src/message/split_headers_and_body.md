---
type: Rust Function
title: split_headers_and_body
resource: crates/lpe-activesync/src/message.rs#L91-L95
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-activesync/src/message/parse_message_part
---

# Signature

`fn split_headers_and_body(raw: &str) -> (&str, &str)`

# Called by

- [parse_message_part](../../../../../functions/crates/lpe-activesync/src/message/parse_message_part.md)