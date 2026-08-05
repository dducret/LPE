---
type: Rust Function
title: canonical_system_mailbox_display_name
resource: crates/lpe-storage/src/util.rs#L91-L93
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-domain/src/mailbox_name/MailboxNamePolicy/canonical_system_display_name
  called_by:
  - functions/crates/lpe-storage/src/mailboxes/Storage/create_jmap_mailbox
  - functions/crates/lpe-storage/src/mailboxes/Storage/create_imap_mailbox
---

# Signature

`pub(crate) fn canonical_system_mailbox_display_name(role: &str) -> Option<&'static str>`

# Calls

- [canonical_system_display_name](../../../../../functions/crates/lpe-domain/src/mailbox_name/MailboxNamePolicy/canonical_system_display_name.md)

# Called by

- [create_jmap_mailbox](../../../../../functions/crates/lpe-storage/src/mailboxes/Storage/create_jmap_mailbox.md)
- [create_imap_mailbox](../../../../../functions/crates/lpe-storage/src/mailboxes/Storage/create_imap_mailbox.md)