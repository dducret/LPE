---
type: Rust Function
title: mailbox_account_may_write
resource: crates/lpe-jmap/src/mailboxes.rs#L548-L550
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-jmap/src/mail/JmapService/handle_email_copy
  - functions/crates/lpe-jmap/src/mail/JmapService/handle_email_import
  - functions/crates/lpe-jmap/src/mail/imports/JmapService/ensure_target_mailbox_accepts_message_write
  - functions/crates/lpe-jmap/src/mailboxes/JmapService/handle_mailbox_set
  - functions/crates/lpe-jmap/src/mailboxes/mailbox_to_value
  - functions/crates/lpe-jmap/src/mailboxes/mailbox_account_may_draft
  - functions/crates/lpe-jmap/src/mailboxes/ensure_mailbox_draft_write
  - functions/crates/lpe-jmap/src/service/helpers/mailbox_state_fingerprint
---

# Signature

`pub(crate) fn mailbox_account_may_write(access: &MailboxAccountAccess) -> bool`

# Called by

- [handle_email_copy](../../../../../functions/crates/lpe-jmap/src/mail/JmapService/handle_email_copy.md)
- [handle_email_import](../../../../../functions/crates/lpe-jmap/src/mail/JmapService/handle_email_import.md)
- [ensure_target_mailbox_accepts_message_write](../../../../../functions/crates/lpe-jmap/src/mail/imports/JmapService/ensure_target_mailbox_accepts_message_write.md)
- [handle_mailbox_set](../../../../../functions/crates/lpe-jmap/src/mailboxes/JmapService/handle_mailbox_set.md)
- [mailbox_to_value](../../../../../functions/crates/lpe-jmap/src/mailboxes/mailbox_to_value.md)
- [mailbox_account_may_draft](../../../../../functions/crates/lpe-jmap/src/mailboxes/mailbox_account_may_draft.md)
- [ensure_mailbox_draft_write](../../../../../functions/crates/lpe-jmap/src/mailboxes/ensure_mailbox_draft_write.md)
- [mailbox_state_fingerprint](../../../../../functions/crates/lpe-jmap/src/service/helpers/mailbox_state_fingerprint.md)