---
type: Rust Function
title: parse_required_string
resource: crates/lpe-jmap/src/parse.rs#L77-L83
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-jmap/src/calendar/parse_calendar_collection_name
  - functions/crates/lpe-jmap/src/calendar/parse_calendar_event_input
  - functions/crates/lpe-jmap/src/calendar/parse_calendar_attachment_inputs
  - functions/crates/lpe-jmap/src/notes_journal/parse_note_input
  - functions/crates/lpe-jmap/src/notes_journal/parse_journal_entry_input
  - functions/crates/lpe-jmap/src/tasks/parse_task_input
  - functions/crates/lpe-jmap/src/tasks/parse_task_list_create
---

# Signature

`pub(crate) fn parse_required_string(value: Option<&Value>, field_name: &str) -> Result<String>`

# Called by

- [parse_calendar_collection_name](../../../../../functions/crates/lpe-jmap/src/calendar/parse_calendar_collection_name.md)
- [parse_calendar_event_input](../../../../../functions/crates/lpe-jmap/src/calendar/parse_calendar_event_input.md)
- [parse_calendar_attachment_inputs](../../../../../functions/crates/lpe-jmap/src/calendar/parse_calendar_attachment_inputs.md)
- [parse_note_input](../../../../../functions/crates/lpe-jmap/src/notes_journal/parse_note_input.md)
- [parse_journal_entry_input](../../../../../functions/crates/lpe-jmap/src/notes_journal/parse_journal_entry_input.md)
- [parse_task_input](../../../../../functions/crates/lpe-jmap/src/tasks/parse_task_input.md)
- [parse_task_list_create](../../../../../functions/crates/lpe-jmap/src/tasks/parse_task_list_create.md)