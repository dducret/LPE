---
type: Rust Function
title: resolve_message_indexes
resource: crates/lpe-imap/src/render.rs#L1146-L1183
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-imap/src/render/find_message_index
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-imap/src/mailboxes/Session/handle_uid_expunge
  - functions/crates/lpe-imap/src/messages/Session/handle_fetch
  - functions/crates/lpe-imap/src/messages/Session/handle_store
  - functions/crates/lpe-imap/src/messages/Session/handle_copy
  - functions/crates/lpe-imap/src/messages/Session/handle_move
---

# Signature

`pub(crate) fn resolve_message_indexes( emails: &[ImapEmail], set_token: &str, ref_kind: MessageRefKind, ) -> Result<Vec<usize>>`

# Calls

- [find_message_index](../../../../../functions/crates/lpe-imap/src/render/find_message_index.md)
- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [handle_uid_expunge](../../../../../functions/crates/lpe-imap/src/mailboxes/Session/handle_uid_expunge.md)
- [handle_fetch](../../../../../functions/crates/lpe-imap/src/messages/Session/handle_fetch.md)
- [handle_store](../../../../../functions/crates/lpe-imap/src/messages/Session/handle_store.md)
- [handle_copy](../../../../../functions/crates/lpe-imap/src/messages/Session/handle_copy.md)
- [handle_move](../../../../../functions/crates/lpe-imap/src/messages/Session/handle_move.md)