---
type: Rust Method
title: object_changes_response
resource: crates/lpe-jmap/src/service/object_state.rs#L13-L60
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/state/state_cursor
  - functions/crates/lpe-jmap/src/state/changes_response_from_durable_with_cursor
  - functions/crates/lpe-jmap/src/state/changes_response_with_cursor
  called_by:
  - functions/crates/lpe-jmap/src/calendar/JmapService/handle_calendar_changes
  - functions/crates/lpe-jmap/src/calendar/JmapService/handle_calendar_event_changes
  - functions/crates/lpe-jmap/src/contacts/JmapService/handle_address_book_changes
  - functions/crates/lpe-jmap/src/contacts/JmapService/handle_contact_changes
  - functions/crates/lpe-jmap/src/mail/JmapService/handle_email_submission_changes
  - functions/crates/lpe-jmap/src/notes_journal/JmapService/handle_note_changes
  - functions/crates/lpe-jmap/src/notes_journal/JmapService/handle_journal_entry_changes
  - functions/crates/lpe-jmap/src/service/canonical/JmapService/handle_canonical_changes
  - functions/crates/lpe-jmap/src/tasks/JmapService/handle_task_list_changes
  - functions/crates/lpe-jmap/src/tasks/JmapService/handle_task_changes
---

# Signature

`pub(crate) async fn object_changes_response( &self, account_id: Uuid, data_type: &str, since_state: &str, max_changes: Option<u64>, entries: Vec<StateEntry>, ) -> Result<Value>`

# Calls

- [state_cursor](../../../../../../../functions/crates/lpe-jmap/src/state/state_cursor.md)
- [changes_response_from_durable_with_cursor](../../../../../../../functions/crates/lpe-jmap/src/state/changes_response_from_durable_with_cursor.md)
- [changes_response_with_cursor](../../../../../../../functions/crates/lpe-jmap/src/state/changes_response_with_cursor.md)

# Called by

- [handle_calendar_changes](../../../../../../../functions/crates/lpe-jmap/src/calendar/JmapService/handle_calendar_changes.md)
- [handle_calendar_event_changes](../../../../../../../functions/crates/lpe-jmap/src/calendar/JmapService/handle_calendar_event_changes.md)
- [handle_address_book_changes](../../../../../../../functions/crates/lpe-jmap/src/contacts/JmapService/handle_address_book_changes.md)
- [handle_contact_changes](../../../../../../../functions/crates/lpe-jmap/src/contacts/JmapService/handle_contact_changes.md)
- [handle_email_submission_changes](../../../../../../../functions/crates/lpe-jmap/src/mail/JmapService/handle_email_submission_changes.md)
- [handle_note_changes](../../../../../../../functions/crates/lpe-jmap/src/notes_journal/JmapService/handle_note_changes.md)
- [handle_journal_entry_changes](../../../../../../../functions/crates/lpe-jmap/src/notes_journal/JmapService/handle_journal_entry_changes.md)
- [handle_canonical_changes](../../../../../../../functions/crates/lpe-jmap/src/service/canonical/JmapService/handle_canonical_changes.md)
- [handle_task_list_changes](../../../../../../../functions/crates/lpe-jmap/src/tasks/JmapService/handle_task_list_changes.md)
- [handle_task_changes](../../../../../../../functions/crates/lpe-jmap/src/tasks/JmapService/handle_task_changes.md)