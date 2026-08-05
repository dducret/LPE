---
type: Rust Function
title: normalize_message_id
resource: crates/lpe-imap/src/append.rs#L283-L285
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-imap/src/append/message_ids_match
---

# Signature

`fn normalize_message_id(value: &str) -> &str`

# Called by

- [message_ids_match](../../../../../functions/crates/lpe-imap/src/append/message_ids_match.md)