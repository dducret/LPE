---
type: Rust Function
title: ensure_mailbox_draft_write
resource: crates/lpe-jmap/src/mailboxes.rs#L569-L576
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/mailboxes/ensure_mailbox_write
  - functions/crates/lpe-jmap/src/mailboxes/mailbox_account_may_write
  - functions/crates/lpe-jmap/src/mailboxes/mailbox_account_may_submit
  called_by:
  - functions/crates/lpe-jmap/src/mail/JmapService/handle_email_set
  - functions/crates/lpe-jmap/src/mail/imports/JmapService/ensure_target_mailbox_accepts_message_write
---

# Signature

`pub(crate) fn ensure_mailbox_draft_write(access: &MailboxAccountAccess) -> Result<()>`

# Calls

- [ensure_mailbox_write](../../../../../functions/crates/lpe-jmap/src/mailboxes/ensure_mailbox_write.md)
- [mailbox_account_may_write](../../../../../functions/crates/lpe-jmap/src/mailboxes/mailbox_account_may_write.md)
- [mailbox_account_may_submit](../../../../../functions/crates/lpe-jmap/src/mailboxes/mailbox_account_may_submit.md)

# Called by

- [handle_email_set](../../../../../functions/crates/lpe-jmap/src/mail/JmapService/handle_email_set.md)
- [ensure_target_mailbox_accepts_message_write](../../../../../functions/crates/lpe-jmap/src/mail/imports/JmapService/ensure_target_mailbox_accepts_message_write.md)