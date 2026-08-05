---
type: Rust Method
title: mail_object_state_entries
resource: crates/lpe-jmap/src/service/object_state.rs#L171-L178
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/service/object_state/JmapService/mail_object_state_entries_with_bcc
  called_by:
  - functions/crates/lpe-jmap/src/mail/JmapService/handle_email_changes
  - functions/crates/lpe-jmap/src/mail/JmapService/handle_thread_changes
  - functions/crates/lpe-jmap/src/service/object_state/JmapService/mail_object_state
---

# Signature

`pub(crate) async fn mail_object_state_entries( &self, access: &MailboxAccountAccess, data_type: &str, ) -> Result<Vec<StateEntry>>`

# Calls

- [mail_object_state_entries_with_bcc](../../../../../../../functions/crates/lpe-jmap/src/service/object_state/JmapService/mail_object_state_entries_with_bcc.md)

# Called by

- [handle_email_changes](../../../../../../../functions/crates/lpe-jmap/src/mail/JmapService/handle_email_changes.md)
- [handle_thread_changes](../../../../../../../functions/crates/lpe-jmap/src/mail/JmapService/handle_thread_changes.md)
- [mail_object_state](../../../../../../../functions/crates/lpe-jmap/src/service/object_state/JmapService/mail_object_state.md)