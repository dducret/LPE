---
type: Rust Method
title: mailbox_object_state
resource: crates/lpe-jmap/src/service/object_state.rs#L62-L72
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/service/object_state/JmapService/mailbox_object_state_entries
  - functions/crates/lpe-jmap/src/state/encode_state_with_cursor
  called_by:
  - functions/crates/lpe-jmap/src/mailboxes/JmapService/handle_mailbox_get
  - functions/crates/lpe-jmap/src/mailboxes/JmapService/handle_mailbox_set
  - functions/crates/lpe-jmap/src/service/canonical/JmapService/canonical_object_state
  - functions/crates/lpe-jmap/src/websocket/JmapService/mail_push_type_state
---

# Signature

`pub(crate) async fn mailbox_object_state( &self, access: &MailboxAccountAccess, ) -> Result<String>`

# Calls

- [mailbox_object_state_entries](../../../../../../../functions/crates/lpe-jmap/src/service/object_state/JmapService/mailbox_object_state_entries.md)
- [encode_state_with_cursor](../../../../../../../functions/crates/lpe-jmap/src/state/encode_state_with_cursor.md)

# Called by

- [handle_mailbox_get](../../../../../../../functions/crates/lpe-jmap/src/mailboxes/JmapService/handle_mailbox_get.md)
- [handle_mailbox_set](../../../../../../../functions/crates/lpe-jmap/src/mailboxes/JmapService/handle_mailbox_set.md)
- [canonical_object_state](../../../../../../../functions/crates/lpe-jmap/src/service/canonical/JmapService/canonical_object_state.md)
- [mail_push_type_state](../../../../../../../functions/crates/lpe-jmap/src/websocket/JmapService/mail_push_type_state.md)