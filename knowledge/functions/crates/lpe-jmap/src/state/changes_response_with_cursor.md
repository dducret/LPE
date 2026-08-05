---
type: Rust Function
title: changes_response_with_cursor
resource: crates/lpe-jmap/src/state.rs#L164-L237
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/state/decode_state
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-jmap/src/state/finish_changes_response
  called_by:
  - functions/crates/lpe-jmap/src/mail/JmapService/handle_email_changes
  - functions/crates/lpe-jmap/src/mail/JmapService/handle_thread_changes
  - functions/crates/lpe-jmap/src/mailboxes/JmapService/handle_mailbox_changes
  - functions/crates/lpe-jmap/src/service/canonical/JmapService/string_object_changes_response
  - functions/crates/lpe-jmap/src/service/object_state/JmapService/object_changes_response
  - functions/crates/lpe-jmap/src/state/changes_response
  - functions/crates/lpe-jmap/src/state/state_tokens_preserve_optional_change_log_cursor
  - functions/crates/lpe-jmap/src/state/truncated_changes_do_not_advance_change_log_cursor
---

# Signature

`pub(crate) fn changes_response_with_cursor( account_id: Uuid, kind: &str, since_state: &str, max_changes: Option<u64>, current_entries: Vec<StateEntry>, current_cursor: Option<i64>, ) -> Result<Value>`

# Calls

- [decode_state](../../../../../functions/crates/lpe-jmap/src/state/decode_state.md)
- [get](../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [finish_changes_response](../../../../../functions/crates/lpe-jmap/src/state/finish_changes_response.md)

# Called by

- [handle_email_changes](../../../../../functions/crates/lpe-jmap/src/mail/JmapService/handle_email_changes.md)
- [handle_thread_changes](../../../../../functions/crates/lpe-jmap/src/mail/JmapService/handle_thread_changes.md)
- [handle_mailbox_changes](../../../../../functions/crates/lpe-jmap/src/mailboxes/JmapService/handle_mailbox_changes.md)
- [string_object_changes_response](../../../../../functions/crates/lpe-jmap/src/service/canonical/JmapService/string_object_changes_response.md)
- [object_changes_response](../../../../../functions/crates/lpe-jmap/src/service/object_state/JmapService/object_changes_response.md)
- [changes_response](../../../../../functions/crates/lpe-jmap/src/state/changes_response.md)
- [state_tokens_preserve_optional_change_log_cursor](../../../../../functions/crates/lpe-jmap/src/state/state_tokens_preserve_optional_change_log_cursor.md)
- [truncated_changes_do_not_advance_change_log_cursor](../../../../../functions/crates/lpe-jmap/src/state/truncated_changes_do_not_advance_change_log_cursor.md)