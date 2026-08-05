---
type: Rust Function
title: parse_optional_string
resource: crates/lpe-jmap/src/parse.rs#L68-L75
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-jmap/src/calendar/parse_calendar_event_input
  - functions/crates/lpe-jmap/src/calendar/parse_calendar_attachment_inputs
  - functions/crates/lpe-jmap/src/drafts/parse_draft_mutation
  - functions/crates/lpe-jmap/src/notes_journal/parse_note_input
  - functions/crates/lpe-jmap/src/notes_journal/parse_journal_entry_input
  - functions/crates/lpe-jmap/src/tasks/parse_task_input
  - functions/crates/lpe-jmap/src/tasks/parse_task_list_update
---

# Signature

`pub(crate) fn parse_optional_string(value: Option<&Value>) -> Result<Option<String>>`

# Called by

- [parse_calendar_event_input](../../../../../functions/crates/lpe-jmap/src/calendar/parse_calendar_event_input.md)
- [parse_calendar_attachment_inputs](../../../../../functions/crates/lpe-jmap/src/calendar/parse_calendar_attachment_inputs.md)
- [parse_draft_mutation](../../../../../functions/crates/lpe-jmap/src/drafts/parse_draft_mutation.md)
- [parse_note_input](../../../../../functions/crates/lpe-jmap/src/notes_journal/parse_note_input.md)
- [parse_journal_entry_input](../../../../../functions/crates/lpe-jmap/src/notes_journal/parse_journal_entry_input.md)
- [parse_task_input](../../../../../functions/crates/lpe-jmap/src/tasks/parse_task_input.md)
- [parse_task_list_update](../../../../../functions/crates/lpe-jmap/src/tasks/parse_task_list_update.md)