---
type: Rust Function
title: state_cursor
resource: crates/lpe-jmap/src/state.rs#L76-L88
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/state/decode_state
  called_by:
  - functions/crates/lpe-jmap/src/mail/JmapService/handle_email_changes
  - functions/crates/lpe-jmap/src/mail/JmapService/handle_thread_changes
  - functions/crates/lpe-jmap/src/mailboxes/JmapService/handle_mailbox_changes
  - functions/crates/lpe-jmap/src/service/canonical/JmapService/string_object_changes_response
  - functions/crates/lpe-jmap/src/service/object_state/JmapService/object_changes_response
---

# Signature

`pub(crate) fn state_cursor(account_id: Uuid, kind: &str, since_state: &str) -> Result<Option<i64>>`

# Calls

- [decode_state](../../../../../functions/crates/lpe-jmap/src/state/decode_state.md)

# Called by

- [handle_email_changes](../../../../../functions/crates/lpe-jmap/src/mail/JmapService/handle_email_changes.md)
- [handle_thread_changes](../../../../../functions/crates/lpe-jmap/src/mail/JmapService/handle_thread_changes.md)
- [handle_mailbox_changes](../../../../../functions/crates/lpe-jmap/src/mailboxes/JmapService/handle_mailbox_changes.md)
- [string_object_changes_response](../../../../../functions/crates/lpe-jmap/src/service/canonical/JmapService/string_object_changes_response.md)
- [object_changes_response](../../../../../functions/crates/lpe-jmap/src/service/object_state/JmapService/object_changes_response.md)