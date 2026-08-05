---
type: Rust Function
title: encode_query_state_reference
resource: crates/lpe-jmap/src/state.rs#L438-L455
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/state/encode_query_state_parts
  called_by:
  - functions/crates/lpe-jmap/src/mail/JmapService/handle_email_query
  - functions/crates/lpe-jmap/src/mail/JmapService/handle_email_query_changes
  - functions/crates/lpe-jmap/src/mailboxes/JmapService/handle_mailbox_query
  - functions/crates/lpe-jmap/src/mailboxes/JmapService/handle_mailbox_query_changes
  - functions/crates/lpe-jmap/src/notes_journal/JmapService/handle_reminder_query
  - functions/crates/lpe-jmap/src/service/canonical/JmapService/handle_canonical_query
  - functions/crates/lpe-jmap/src/service/canonical/JmapService/handle_canonical_query_changes
---

# Signature

`pub(crate) fn encode_query_state_reference( account_id: Uuid, kind: &str, filter: Option<Value>, sort: Option<Vec<Value>>, state_id: Uuid, cursor: i64, ) -> Result<String>`

# Calls

- [encode_query_state_parts](../../../../../functions/crates/lpe-jmap/src/state/encode_query_state_parts.md)

# Called by

- [handle_email_query](../../../../../functions/crates/lpe-jmap/src/mail/JmapService/handle_email_query.md)
- [handle_email_query_changes](../../../../../functions/crates/lpe-jmap/src/mail/JmapService/handle_email_query_changes.md)
- [handle_mailbox_query](../../../../../functions/crates/lpe-jmap/src/mailboxes/JmapService/handle_mailbox_query.md)
- [handle_mailbox_query_changes](../../../../../functions/crates/lpe-jmap/src/mailboxes/JmapService/handle_mailbox_query_changes.md)
- [handle_reminder_query](../../../../../functions/crates/lpe-jmap/src/notes_journal/JmapService/handle_reminder_query.md)
- [handle_canonical_query](../../../../../functions/crates/lpe-jmap/src/service/canonical/JmapService/handle_canonical_query.md)
- [handle_canonical_query_changes](../../../../../functions/crates/lpe-jmap/src/service/canonical/JmapService/handle_canonical_query_changes.md)