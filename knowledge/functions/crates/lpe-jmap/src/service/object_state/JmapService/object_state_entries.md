---
type: Rust Method
title: object_state_entries
resource: crates/lpe-jmap/src/service/object_state.rs#L238-L395
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/service/helpers/mailbox_state_fingerprint
  - functions/crates/lpe-jmap/src/service/object_state/JmapService/mail_object_state_entries_with_bcc
  - functions/crates/lpe-jmap/src/service/helpers/collection_state_fingerprint
  - functions/crates/lpe-jmap/src/service/helpers/contact_state_fingerprint
  - functions/crates/lpe-jmap/src/service/helpers/event_state_fingerprint
  - functions/crates/lpe-jmap/src/service/helpers/task_list_state_fingerprint
  - functions/crates/lpe-jmap/src/service/helpers/task_state_fingerprint
  - functions/crates/lpe-jmap/src/notes_journal/note_state_fingerprint
  - functions/crates/lpe-jmap/src/notes_journal/journal_entry_state_fingerprint
  - functions/crates/lpe-jmap/src/notes_journal/reminder_state_fingerprint
  - functions/crates/lpe-jmap/src/service/helpers/opaque_state_fingerprint
  - functions/crates/lpe-jmap/src/service/helpers/rule_to_value
  - functions/crates/lpe-jmap/src/service/helpers/search_folder_to_value
  called_by:
  - functions/crates/lpe-jmap/src/calendar/JmapService/handle_calendar_event_changes
  - functions/crates/lpe-jmap/src/contacts/JmapService/handle_contact_changes
  - functions/crates/lpe-jmap/src/notes_journal/JmapService/handle_note_changes
  - functions/crates/lpe-jmap/src/notes_journal/JmapService/handle_journal_entry_changes
  - functions/crates/lpe-jmap/src/service/canonical/JmapService/canonical_objects
  - functions/crates/lpe-jmap/src/service/object_state/JmapService/object_state
  - functions/crates/lpe-jmap/src/tasks/JmapService/handle_task_list_changes
  - functions/crates/lpe-jmap/src/tasks/JmapService/handle_task_changes
---

# Signature

`pub(crate) async fn object_state_entries( &self, account_id: Uuid, data_type: &str, ) -> Result<Vec<StateEntry>>`

# Calls

- [mailbox_state_fingerprint](../../../../../../../functions/crates/lpe-jmap/src/service/helpers/mailbox_state_fingerprint.md)
- [mail_object_state_entries_with_bcc](../../../../../../../functions/crates/lpe-jmap/src/service/object_state/JmapService/mail_object_state_entries_with_bcc.md)
- [collection_state_fingerprint](../../../../../../../functions/crates/lpe-jmap/src/service/helpers/collection_state_fingerprint.md)
- [contact_state_fingerprint](../../../../../../../functions/crates/lpe-jmap/src/service/helpers/contact_state_fingerprint.md)
- [event_state_fingerprint](../../../../../../../functions/crates/lpe-jmap/src/service/helpers/event_state_fingerprint.md)
- [task_list_state_fingerprint](../../../../../../../functions/crates/lpe-jmap/src/service/helpers/task_list_state_fingerprint.md)
- [task_state_fingerprint](../../../../../../../functions/crates/lpe-jmap/src/service/helpers/task_state_fingerprint.md)
- [note_state_fingerprint](../../../../../../../functions/crates/lpe-jmap/src/notes_journal/note_state_fingerprint.md)
- [journal_entry_state_fingerprint](../../../../../../../functions/crates/lpe-jmap/src/notes_journal/journal_entry_state_fingerprint.md)
- [reminder_state_fingerprint](../../../../../../../functions/crates/lpe-jmap/src/notes_journal/reminder_state_fingerprint.md)
- [opaque_state_fingerprint](../../../../../../../functions/crates/lpe-jmap/src/service/helpers/opaque_state_fingerprint.md)
- [rule_to_value](../../../../../../../functions/crates/lpe-jmap/src/service/helpers/rule_to_value.md)
- [search_folder_to_value](../../../../../../../functions/crates/lpe-jmap/src/service/helpers/search_folder_to_value.md)

# Called by

- [handle_calendar_event_changes](../../../../../../../functions/crates/lpe-jmap/src/calendar/JmapService/handle_calendar_event_changes.md)
- [handle_contact_changes](../../../../../../../functions/crates/lpe-jmap/src/contacts/JmapService/handle_contact_changes.md)
- [handle_note_changes](../../../../../../../functions/crates/lpe-jmap/src/notes_journal/JmapService/handle_note_changes.md)
- [handle_journal_entry_changes](../../../../../../../functions/crates/lpe-jmap/src/notes_journal/JmapService/handle_journal_entry_changes.md)
- [canonical_objects](../../../../../../../functions/crates/lpe-jmap/src/service/canonical/JmapService/canonical_objects.md)
- [object_state](../../../../../../../functions/crates/lpe-jmap/src/service/object_state/JmapService/object_state.md)
- [handle_task_list_changes](../../../../../../../functions/crates/lpe-jmap/src/tasks/JmapService/handle_task_list_changes.md)
- [handle_task_changes](../../../../../../../functions/crates/lpe-jmap/src/tasks/JmapService/handle_task_changes.md)