---
type: Rust Function
title: ensure_move_allowed
resource: crates/lpe-imap/src/messages.rs#L498-L512
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-imap/src/messages/Session/handle_move
---

# Signature

`fn ensure_move_allowed(selected: &SelectedMailbox, target_mailbox: &JmapMailbox) -> Result<()>`

# Called by

- [handle_move](../../../../../functions/crates/lpe-imap/src/messages/Session/handle_move.md)