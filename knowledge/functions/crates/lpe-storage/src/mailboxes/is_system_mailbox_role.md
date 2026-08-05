---
type: Rust Function
title: is_system_mailbox_role
resource: crates/lpe-storage/src/mailboxes.rs#L1355-L1358
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/src/mailboxes/Storage/update_jmap_mailbox
  - functions/crates/lpe-storage/src/mailboxes/Storage/rename_imap_mailbox
  - functions/crates/lpe-storage/src/mailboxes/Storage/destroy_jmap_mailbox
---

# Signature

`fn is_system_mailbox_role(role: &str) -> bool`

# Called by

- [update_jmap_mailbox](../../../../../functions/crates/lpe-storage/src/mailboxes/Storage/update_jmap_mailbox.md)
- [rename_imap_mailbox](../../../../../functions/crates/lpe-storage/src/mailboxes/Storage/rename_imap_mailbox.md)
- [destroy_jmap_mailbox](../../../../../functions/crates/lpe-storage/src/mailboxes/Storage/destroy_jmap_mailbox.md)