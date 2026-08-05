---
type: Rust Method
title: mailbox_object_state_entries
resource: crates/lpe-jmap/src/service/object_state.rs#L74-L86
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/service/helpers/mailbox_state_fingerprint
  called_by:
  - functions/crates/lpe-jmap/src/mailboxes/JmapService/handle_mailbox_changes
  - functions/crates/lpe-jmap/src/service/object_state/JmapService/mailbox_object_state
---

# Signature

`pub(crate) async fn mailbox_object_state_entries( &self, access: &MailboxAccountAccess, ) -> Result<Vec<StateEntry>>`

# Calls

- [mailbox_state_fingerprint](../../../../../../../functions/crates/lpe-jmap/src/service/helpers/mailbox_state_fingerprint.md)

# Called by

- [handle_mailbox_changes](../../../../../../../functions/crates/lpe-jmap/src/mailboxes/JmapService/handle_mailbox_changes.md)
- [mailbox_object_state](../../../../../../../functions/crates/lpe-jmap/src/service/object_state/JmapService/mailbox_object_state.md)