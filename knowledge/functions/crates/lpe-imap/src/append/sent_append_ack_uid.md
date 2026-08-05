---
type: Rust Function
title: sent_append_ack_uid
resource: crates/lpe-imap/src/append.rs#L260-L277
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-imap/src/append/message_ids_match
  called_by:
  - functions/crates/lpe-imap/src/append/Session/handle_append
---

# Signature

`fn sent_append_ack_uid( sent_messages: &[lpe_storage::ImapEmail], appended_message_id: Option<&str>, ) -> Option<u32>`

# Calls

- [message_ids_match](../../../../../functions/crates/lpe-imap/src/append/message_ids_match.md)

# Called by

- [handle_append](../../../../../functions/crates/lpe-imap/src/append/Session/handle_append.md)