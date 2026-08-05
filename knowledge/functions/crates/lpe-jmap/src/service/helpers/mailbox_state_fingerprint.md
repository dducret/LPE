---
type: Rust Function
title: mailbox_state_fingerprint
resource: crates/lpe-jmap/src/service/helpers.rs#L703-L739
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/mailboxes/mailbox_account_may_write
  - functions/crates/lpe-jmap/src/mailboxes/mailbox_account_may_submit
  - functions/crates/lpe-jmap/src/service/helpers/opaque_state_fingerprint
  called_by:
  - functions/crates/lpe-jmap/src/service/object_state/JmapService/mailbox_object_state_entries
  - functions/crates/lpe-jmap/src/service/object_state/JmapService/object_state_entries
---

# Signature

`pub(super) fn mailbox_state_fingerprint( mailbox: &JmapMailbox, access: Option<&MailboxAccountAccess>, ) -> String`

# Calls

- [mailbox_account_may_write](../../../../../../functions/crates/lpe-jmap/src/mailboxes/mailbox_account_may_write.md)
- [mailbox_account_may_submit](../../../../../../functions/crates/lpe-jmap/src/mailboxes/mailbox_account_may_submit.md)
- [opaque_state_fingerprint](../../../../../../functions/crates/lpe-jmap/src/service/helpers/opaque_state_fingerprint.md)

# Called by

- [mailbox_object_state_entries](../../../../../../functions/crates/lpe-jmap/src/service/object_state/JmapService/mailbox_object_state_entries.md)
- [object_state_entries](../../../../../../functions/crates/lpe-jmap/src/service/object_state/JmapService/object_state_entries.md)