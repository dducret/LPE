---
type: Rust Method
title: ensure_target_mailbox_accepts_message_write
resource: crates/lpe-jmap/src/mail/imports.rs#L102-L124
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/mailboxes/ensure_mailbox_write
  - functions/crates/lpe-jmap/src/mailboxes/mailbox_account_may_write
  - functions/crates/lpe-jmap/src/mailboxes/ensure_mailbox_draft_write
  called_by:
  - functions/crates/lpe-jmap/src/mail/JmapService/handle_email_copy
  - functions/crates/lpe-jmap/src/mail/imports/JmapService/parse_email_import
---

# Signature

`pub(crate) async fn ensure_target_mailbox_accepts_message_write( &self, account_id: Uuid, target_mailbox_id: Uuid, account_access: &MailboxAccountAccess, ) -> Result<()>`

# Calls

- [ensure_mailbox_write](../../../../../../../functions/crates/lpe-jmap/src/mailboxes/ensure_mailbox_write.md)
- [mailbox_account_may_write](../../../../../../../functions/crates/lpe-jmap/src/mailboxes/mailbox_account_may_write.md)
- [ensure_mailbox_draft_write](../../../../../../../functions/crates/lpe-jmap/src/mailboxes/ensure_mailbox_draft_write.md)

# Called by

- [handle_email_copy](../../../../../../../functions/crates/lpe-jmap/src/mail/JmapService/handle_email_copy.md)
- [parse_email_import](../../../../../../../functions/crates/lpe-jmap/src/mail/imports/JmapService/parse_email_import.md)