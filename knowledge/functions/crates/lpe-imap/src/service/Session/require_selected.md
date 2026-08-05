---
type: Rust Method
title: require_selected
resource: crates/lpe-imap/src/service.rs#L600-L604
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-imap/src/mailboxes/Session/handle_check
  - functions/crates/lpe-imap/src/mailboxes/Session/handle_close
  - functions/crates/lpe-imap/src/mailboxes/Session/handle_unselect
  - functions/crates/lpe-imap/src/mailboxes/Session/handle_expunge
  - functions/crates/lpe-imap/src/mailboxes/Session/handle_uid_expunge
  - functions/crates/lpe-imap/src/messages/Session/handle_fetch
  - functions/crates/lpe-imap/src/messages/Session/handle_store
  - functions/crates/lpe-imap/src/messages/Session/handle_search
  - functions/crates/lpe-imap/src/messages/Session/handle_copy
  - functions/crates/lpe-imap/src/messages/Session/handle_move
---

# Signature

`pub(crate) fn require_selected(&self) -> Result<&SelectedMailbox>`

# Called by

- [handle_check](../../../../../../functions/crates/lpe-imap/src/mailboxes/Session/handle_check.md)
- [handle_close](../../../../../../functions/crates/lpe-imap/src/mailboxes/Session/handle_close.md)
- [handle_unselect](../../../../../../functions/crates/lpe-imap/src/mailboxes/Session/handle_unselect.md)
- [handle_expunge](../../../../../../functions/crates/lpe-imap/src/mailboxes/Session/handle_expunge.md)
- [handle_uid_expunge](../../../../../../functions/crates/lpe-imap/src/mailboxes/Session/handle_uid_expunge.md)
- [handle_fetch](../../../../../../functions/crates/lpe-imap/src/messages/Session/handle_fetch.md)
- [handle_store](../../../../../../functions/crates/lpe-imap/src/messages/Session/handle_store.md)
- [handle_search](../../../../../../functions/crates/lpe-imap/src/messages/Session/handle_search.md)
- [handle_copy](../../../../../../functions/crates/lpe-imap/src/messages/Session/handle_copy.md)
- [handle_move](../../../../../../functions/crates/lpe-imap/src/messages/Session/handle_move.md)