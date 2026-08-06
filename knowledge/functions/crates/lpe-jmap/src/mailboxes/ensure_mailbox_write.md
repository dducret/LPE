---
type: Rust Function
title: ensure_mailbox_write
resource: crates/lpe-jmap/src/mailboxes.rs#L561-L567
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-jmap/src/mail/JmapService/handle_email_copy
  - functions/crates/lpe-jmap/src/mail/JmapService/handle_email_import
  - functions/crates/lpe-jmap/src/mail/imports/JmapService/ensure_target_mailbox_accepts_message_write
  - functions/crates/lpe-jmap/src/mailboxes/JmapService/handle_mailbox_set
  - functions/crates/lpe-jmap/src/mailboxes/ensure_mailbox_draft_write
---

# Signature

`pub(crate) fn ensure_mailbox_write(may_write: bool) -> Result<()>`

# Called by

- [handle_email_copy](../../../../../functions/crates/lpe-jmap/src/mail/JmapService/handle_email_copy.md)
- [handle_email_import](../../../../../functions/crates/lpe-jmap/src/mail/JmapService/handle_email_import.md)
- [ensure_target_mailbox_accepts_message_write](../../../../../functions/crates/lpe-jmap/src/mail/imports/JmapService/ensure_target_mailbox_accepts_message_write.md)
- [handle_mailbox_set](../../../../../functions/crates/lpe-jmap/src/mailboxes/JmapService/handle_mailbox_set.md)
- [ensure_mailbox_draft_write](../../../../../functions/crates/lpe-jmap/src/mailboxes/ensure_mailbox_draft_write.md)