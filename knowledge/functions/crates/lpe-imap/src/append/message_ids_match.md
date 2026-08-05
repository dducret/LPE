---
type: Rust Function
title: message_ids_match
resource: crates/lpe-imap/src/append.rs#L279-L281
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-imap/src/append/normalize_message_id
  called_by:
  - functions/crates/lpe-imap/src/append/sent_append_ack_uid
---

# Signature

`fn message_ids_match(stored: &str, appended: &str) -> bool`

# Calls

- [normalize_message_id](../../../../../functions/crates/lpe-imap/src/append/normalize_message_id.md)

# Called by

- [sent_append_ack_uid](../../../../../functions/crates/lpe-imap/src/append/sent_append_ack_uid.md)