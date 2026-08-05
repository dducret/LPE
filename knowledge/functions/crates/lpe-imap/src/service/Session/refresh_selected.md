---
type: Rust Method
title: refresh_selected
resource: crates/lpe-imap/src/service.rs#L606-L622
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-imap/src/append/Session/handle_append
  - functions/crates/lpe-imap/src/idle/Session/handle_idle
  - functions/crates/lpe-imap/src/mailboxes/Session/handle_expunge
  - functions/crates/lpe-imap/src/mailboxes/Session/handle_uid_expunge
  - functions/crates/lpe-imap/src/messages/Session/handle_fetch
  - functions/crates/lpe-imap/src/messages/Session/handle_store
  - functions/crates/lpe-imap/src/messages/Session/handle_search
  - functions/crates/lpe-imap/src/messages/Session/handle_copy
  - functions/crates/lpe-imap/src/messages/Session/handle_move
  - functions/crates/lpe-imap/src/service/Session/refresh_selected_updates
---

# Signature

`pub(crate) async fn refresh_selected(&mut self) -> Result<()>`

# Called by

- [handle_append](../../../../../../functions/crates/lpe-imap/src/append/Session/handle_append.md)
- [handle_idle](../../../../../../functions/crates/lpe-imap/src/idle/Session/handle_idle.md)
- [handle_expunge](../../../../../../functions/crates/lpe-imap/src/mailboxes/Session/handle_expunge.md)
- [handle_uid_expunge](../../../../../../functions/crates/lpe-imap/src/mailboxes/Session/handle_uid_expunge.md)
- [handle_fetch](../../../../../../functions/crates/lpe-imap/src/messages/Session/handle_fetch.md)
- [handle_store](../../../../../../functions/crates/lpe-imap/src/messages/Session/handle_store.md)
- [handle_search](../../../../../../functions/crates/lpe-imap/src/messages/Session/handle_search.md)
- [handle_copy](../../../../../../functions/crates/lpe-imap/src/messages/Session/handle_copy.md)
- [handle_move](../../../../../../functions/crates/lpe-imap/src/messages/Session/handle_move.md)
- [refresh_selected_updates](../../../../../../functions/crates/lpe-imap/src/service/Session/refresh_selected_updates.md)