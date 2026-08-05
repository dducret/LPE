---
type: Rust Function
title: system_mailbox_role_for_display_name
resource: crates/lpe-storage/src/util.rs#L87-L89
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-domain/src/mailbox_name/MailboxNamePolicy/system_role_for_display_name
  called_by:
  - functions/crates/lpe-storage/src/mailboxes/Storage/create_jmap_mailbox
  - functions/crates/lpe-storage/src/mailboxes/Storage/create_imap_mailbox
---

# Signature

`pub(crate) fn system_mailbox_role_for_display_name(display_name: &str) -> Option<&'static str>`

# Calls

- [system_role_for_display_name](../../../../../functions/crates/lpe-domain/src/mailbox_name/MailboxNamePolicy/system_role_for_display_name.md)

# Called by

- [create_jmap_mailbox](../../../../../functions/crates/lpe-storage/src/mailboxes/Storage/create_jmap_mailbox.md)
- [create_imap_mailbox](../../../../../functions/crates/lpe-storage/src/mailboxes/Storage/create_imap_mailbox.md)