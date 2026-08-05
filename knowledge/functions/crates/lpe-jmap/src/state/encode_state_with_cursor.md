---
type: Rust Function
title: encode_state_with_cursor
resource: crates/lpe-jmap/src/state.rs#L337-L353
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-jmap/src/service/canonical/JmapService/canonical_object_state
  - functions/crates/lpe-jmap/src/service/object_state/JmapService/object_state
  - functions/crates/lpe-jmap/src/service/object_state/JmapService/mailbox_object_state
  - functions/crates/lpe-jmap/src/service/object_state/JmapService/mail_object_state
  - functions/crates/lpe-jmap/src/service/object_state/JmapService/email_submission_object_state
  - functions/crates/lpe-jmap/src/state/finish_changes_response
  - functions/crates/lpe-jmap/src/state/encode_state
  - functions/crates/lpe-jmap/src/state/state_tokens_preserve_optional_change_log_cursor
  - functions/crates/lpe-jmap/src/state/truncated_changes_do_not_advance_change_log_cursor
  - functions/crates/lpe-jmap/src/tests/email_changes_use_durable_log_ids_when_state_has_cursor
  - functions/crates/lpe-jmap/src/tests/thread_changes_use_durable_log_ids_when_state_has_cursor
  - functions/crates/lpe-jmap/src/tests/mailbox_changes_use_durable_log_ids_when_state_has_cursor
  - functions/crates/lpe-jmap/src/tests/email_submission_changes_use_durable_log_ids_when_state_has_cursor
  - functions/crates/lpe-jmap/src/tests/collaboration_changes_use_durable_log_ids_when_state_has_cursor
  - functions/crates/lpe-jmap/src/tests/object_changes_with_cursor_do_not_diff_unlogged_current_state
  - functions/crates/lpe-jmap/src/tests/share_and_reminder_changes_use_string_id_durable_replay
---

# Signature

`pub(crate) fn encode_state_with_cursor( account_id: Uuid, kind: &str, entries: Vec<StateEntry>, cursor: Option<i64>, ) -> Result<String>`

# Called by

- [canonical_object_state](../../../../../functions/crates/lpe-jmap/src/service/canonical/JmapService/canonical_object_state.md)
- [object_state](../../../../../functions/crates/lpe-jmap/src/service/object_state/JmapService/object_state.md)
- [mailbox_object_state](../../../../../functions/crates/lpe-jmap/src/service/object_state/JmapService/mailbox_object_state.md)
- [mail_object_state](../../../../../functions/crates/lpe-jmap/src/service/object_state/JmapService/mail_object_state.md)
- [email_submission_object_state](../../../../../functions/crates/lpe-jmap/src/service/object_state/JmapService/email_submission_object_state.md)
- [finish_changes_response](../../../../../functions/crates/lpe-jmap/src/state/finish_changes_response.md)
- [encode_state](../../../../../functions/crates/lpe-jmap/src/state/encode_state.md)
- [state_tokens_preserve_optional_change_log_cursor](../../../../../functions/crates/lpe-jmap/src/state/state_tokens_preserve_optional_change_log_cursor.md)
- [truncated_changes_do_not_advance_change_log_cursor](../../../../../functions/crates/lpe-jmap/src/state/truncated_changes_do_not_advance_change_log_cursor.md)
- [email_changes_use_durable_log_ids_when_state_has_cursor](../../../../../functions/crates/lpe-jmap/src/tests/email_changes_use_durable_log_ids_when_state_has_cursor.md)
- [thread_changes_use_durable_log_ids_when_state_has_cursor](../../../../../functions/crates/lpe-jmap/src/tests/thread_changes_use_durable_log_ids_when_state_has_cursor.md)
- [mailbox_changes_use_durable_log_ids_when_state_has_cursor](../../../../../functions/crates/lpe-jmap/src/tests/mailbox_changes_use_durable_log_ids_when_state_has_cursor.md)
- [email_submission_changes_use_durable_log_ids_when_state_has_cursor](../../../../../functions/crates/lpe-jmap/src/tests/email_submission_changes_use_durable_log_ids_when_state_has_cursor.md)
- [collaboration_changes_use_durable_log_ids_when_state_has_cursor](../../../../../functions/crates/lpe-jmap/src/tests/collaboration_changes_use_durable_log_ids_when_state_has_cursor.md)
- [object_changes_with_cursor_do_not_diff_unlogged_current_state](../../../../../functions/crates/lpe-jmap/src/tests/object_changes_with_cursor_do_not_diff_unlogged_current_state.md)
- [share_and_reminder_changes_use_string_id_durable_replay](../../../../../functions/crates/lpe-jmap/src/tests/share_and_reminder_changes_use_string_id_durable_replay.md)