---
type: Rust Method
title: collides_with
resource: crates/lpe-domain/src/mailbox_name.rs#L183-L185
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-imap/src/render/mailbox_name_matches
  - functions/crates/lpe-imap/src/tests/mailbox_name_collides
  - functions/crates/lpe-jmap/src/mailboxes/validate_mailbox_set_names
  - functions/crates/lpe-storage/src/inbound/Storage/ensure_named_mailbox
  - functions/crates/lpe-storage/src/mailboxes/Storage/ensure_mailbox_name_available_in_tx
  - functions/crates/lpe-storage/src/mailboxes/Storage/find_mailbox_by_name_in_tx
---

# Signature

`pub fn collides_with(&self, other: &Self) -> bool`

# Called by

- [mailbox_name_matches](../../../../../../functions/crates/lpe-imap/src/render/mailbox_name_matches.md)
- [mailbox_name_collides](../../../../../../functions/crates/lpe-imap/src/tests/mailbox_name_collides.md)
- [validate_mailbox_set_names](../../../../../../functions/crates/lpe-jmap/src/mailboxes/validate_mailbox_set_names.md)
- [ensure_named_mailbox](../../../../../../functions/crates/lpe-storage/src/inbound/Storage/ensure_named_mailbox.md)
- [ensure_mailbox_name_available_in_tx](../../../../../../functions/crates/lpe-storage/src/mailboxes/Storage/ensure_mailbox_name_available_in_tx.md)
- [find_mailbox_by_name_in_tx](../../../../../../functions/crates/lpe-storage/src/mailboxes/Storage/find_mailbox_by_name_in_tx.md)