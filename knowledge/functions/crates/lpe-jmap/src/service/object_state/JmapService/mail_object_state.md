---
type: Rust Method
title: mail_object_state
resource: crates/lpe-jmap/src/service/object_state.rs#L88-L99
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/service/object_state/JmapService/mail_object_state_entries
  - functions/crates/lpe-jmap/src/state/encode_state_with_cursor
  called_by:
  - functions/crates/lpe-jmap/src/mail/JmapService/handle_email_get
  - functions/crates/lpe-jmap/src/mail/JmapService/handle_email_copy
  - functions/crates/lpe-jmap/src/mail/JmapService/handle_email_import
  - functions/crates/lpe-jmap/src/mail/JmapService/handle_email_set
  - functions/crates/lpe-jmap/src/mail/JmapService/handle_thread_get
  - functions/crates/lpe-jmap/src/service/canonical/JmapService/canonical_object_state
  - functions/crates/lpe-jmap/src/tests/delegated_email_and_thread_states_ignore_bcc_only_changes
  - functions/crates/lpe-jmap/src/websocket/JmapService/mail_push_type_state
---

# Signature

`pub(crate) async fn mail_object_state( &self, access: &MailboxAccountAccess, data_type: &str, ) -> Result<String>`

# Calls

- [mail_object_state_entries](../../../../../../../functions/crates/lpe-jmap/src/service/object_state/JmapService/mail_object_state_entries.md)
- [encode_state_with_cursor](../../../../../../../functions/crates/lpe-jmap/src/state/encode_state_with_cursor.md)

# Called by

- [handle_email_get](../../../../../../../functions/crates/lpe-jmap/src/mail/JmapService/handle_email_get.md)
- [handle_email_copy](../../../../../../../functions/crates/lpe-jmap/src/mail/JmapService/handle_email_copy.md)
- [handle_email_import](../../../../../../../functions/crates/lpe-jmap/src/mail/JmapService/handle_email_import.md)
- [handle_email_set](../../../../../../../functions/crates/lpe-jmap/src/mail/JmapService/handle_email_set.md)
- [handle_thread_get](../../../../../../../functions/crates/lpe-jmap/src/mail/JmapService/handle_thread_get.md)
- [canonical_object_state](../../../../../../../functions/crates/lpe-jmap/src/service/canonical/JmapService/canonical_object_state.md)
- [delegated_email_and_thread_states_ignore_bcc_only_changes](../../../../../../../functions/crates/lpe-jmap/src/tests/delegated_email_and_thread_states_ignore_bcc_only_changes.md)
- [mail_push_type_state](../../../../../../../functions/crates/lpe-jmap/src/websocket/JmapService/mail_push_type_state.md)