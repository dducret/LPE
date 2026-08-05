---
type: Rust Function
title: mailbox_account_may_draft
resource: crates/lpe-jmap/src/mailboxes.rs#L549-L551
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/mailboxes/mailbox_account_may_write
  - functions/crates/lpe-jmap/src/mailboxes/mailbox_account_may_submit
  called_by:
  - functions/crates/lpe-jmap/src/mailboxes/mailbox_to_value
---

# Signature

`pub(crate) fn mailbox_account_may_draft(access: &MailboxAccountAccess) -> bool`

# Calls

- [mailbox_account_may_write](../../../../../functions/crates/lpe-jmap/src/mailboxes/mailbox_account_may_write.md)
- [mailbox_account_may_submit](../../../../../functions/crates/lpe-jmap/src/mailboxes/mailbox_account_may_submit.md)

# Called by

- [mailbox_to_value](../../../../../functions/crates/lpe-jmap/src/mailboxes/mailbox_to_value.md)