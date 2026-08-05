---
type: Rust Method
title: object_state
resource: crates/lpe-jmap/src/service/object_state.rs#L4-L11
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/service/object_state/JmapService/object_state_entries
  - functions/crates/lpe-jmap/src/state/encode_state_with_cursor
  called_by:
  - functions/crates/lpe-jmap/src/calendar/JmapService/handle_calendar_get
  - functions/crates/lpe-jmap/src/calendar/JmapService/handle_calendar_set
  - functions/crates/lpe-jmap/src/calendar/JmapService/handle_calendar_event_get
  - functions/crates/lpe-jmap/src/calendar/JmapService/handle_calendar_event_set
  - functions/crates/lpe-jmap/src/contacts/JmapService/handle_address_book_get
  - functions/crates/lpe-jmap/src/contacts/JmapService/handle_contact_get
  - functions/crates/lpe-jmap/src/contacts/JmapService/handle_contact_set
  - functions/crates/lpe-jmap/src/notes_journal/JmapService/handle_note_set
  - functions/crates/lpe-jmap/src/notes_journal/JmapService/handle_journal_entry_set
  - functions/crates/lpe-jmap/src/service/canonical/JmapService/canonical_object_state
  - functions/crates/lpe-jmap/src/tasks/JmapService/handle_task_list_get
  - functions/crates/lpe-jmap/src/tasks/JmapService/handle_task_list_set
  - functions/crates/lpe-jmap/src/tasks/JmapService/handle_task_get
  - functions/crates/lpe-jmap/src/tasks/JmapService/handle_task_set
  - functions/crates/lpe-jmap/src/websocket/JmapService/compute_push_changes
  - functions/crates/lpe-jmap/src/websocket/JmapService/mail_push_type_state
  - functions/crates/lpe-jmap/src/websocket/JmapService/current_push_states
---

# Signature

`pub(crate) async fn object_state(&self, account_id: Uuid, data_type: &str) -> Result<String>`

# Calls

- [object_state_entries](../../../../../../../functions/crates/lpe-jmap/src/service/object_state/JmapService/object_state_entries.md)
- [encode_state_with_cursor](../../../../../../../functions/crates/lpe-jmap/src/state/encode_state_with_cursor.md)

# Called by

- [handle_calendar_get](../../../../../../../functions/crates/lpe-jmap/src/calendar/JmapService/handle_calendar_get.md)
- [handle_calendar_set](../../../../../../../functions/crates/lpe-jmap/src/calendar/JmapService/handle_calendar_set.md)
- [handle_calendar_event_get](../../../../../../../functions/crates/lpe-jmap/src/calendar/JmapService/handle_calendar_event_get.md)
- [handle_calendar_event_set](../../../../../../../functions/crates/lpe-jmap/src/calendar/JmapService/handle_calendar_event_set.md)
- [handle_address_book_get](../../../../../../../functions/crates/lpe-jmap/src/contacts/JmapService/handle_address_book_get.md)
- [handle_contact_get](../../../../../../../functions/crates/lpe-jmap/src/contacts/JmapService/handle_contact_get.md)
- [handle_contact_set](../../../../../../../functions/crates/lpe-jmap/src/contacts/JmapService/handle_contact_set.md)
- [handle_note_set](../../../../../../../functions/crates/lpe-jmap/src/notes_journal/JmapService/handle_note_set.md)
- [handle_journal_entry_set](../../../../../../../functions/crates/lpe-jmap/src/notes_journal/JmapService/handle_journal_entry_set.md)
- [canonical_object_state](../../../../../../../functions/crates/lpe-jmap/src/service/canonical/JmapService/canonical_object_state.md)
- [handle_task_list_get](../../../../../../../functions/crates/lpe-jmap/src/tasks/JmapService/handle_task_list_get.md)
- [handle_task_list_set](../../../../../../../functions/crates/lpe-jmap/src/tasks/JmapService/handle_task_list_set.md)
- [handle_task_get](../../../../../../../functions/crates/lpe-jmap/src/tasks/JmapService/handle_task_get.md)
- [handle_task_set](../../../../../../../functions/crates/lpe-jmap/src/tasks/JmapService/handle_task_set.md)
- [compute_push_changes](../../../../../../../functions/crates/lpe-jmap/src/websocket/JmapService/compute_push_changes.md)
- [mail_push_type_state](../../../../../../../functions/crates/lpe-jmap/src/websocket/JmapService/mail_push_type_state.md)
- [current_push_states](../../../../../../../functions/crates/lpe-jmap/src/websocket/JmapService/current_push_states.md)