---
type: Rust Function
title: mailbox_to_value
resource: crates/lpe-jmap/src/mailboxes.rs#L491-L539
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/mailboxes/mailbox_account_may_draft
  - functions/crates/lpe-jmap/src/convert/insert_if
  called_by:
  - functions/crates/lpe-jmap/src/mailboxes/JmapService/handle_mailbox_get
---

# Signature

`fn mailbox_to_value( mailbox: &JmapMailbox, access: &MailboxAccountAccess, properties: &HashSet<String>, ) -> Value`

# Calls

- [mailbox_account_may_draft](../../../../../functions/crates/lpe-jmap/src/mailboxes/mailbox_account_may_draft.md)
- [insert_if](../../../../../functions/crates/lpe-jmap/src/convert/insert_if.md)

# Called by

- [handle_mailbox_get](../../../../../functions/crates/lpe-jmap/src/mailboxes/JmapService/handle_mailbox_get.md)