---
type: Rust Function
title: parse_uuid_list
resource: crates/lpe-jmap/src/parse.rs#L13-L17
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/parse/parse_uuid
  called_by:
  - functions/crates/lpe-jmap/src/calendar/JmapService/handle_calendar_event_get
  - functions/crates/lpe-jmap/src/contacts/JmapService/handle_contact_get
  - functions/crates/lpe-jmap/src/mail/JmapService/handle_email_get
  - functions/crates/lpe-jmap/src/mail/JmapService/handle_email_submission_get
  - functions/crates/lpe-jmap/src/mail/JmapService/handle_search_snippet_get
  - functions/crates/lpe-jmap/src/mailboxes/JmapService/handle_mailbox_get
  - functions/crates/lpe-jmap/src/notes_journal/JmapService/handle_note_get
  - functions/crates/lpe-jmap/src/notes_journal/JmapService/handle_journal_entry_get
  - functions/crates/lpe-jmap/src/tasks/JmapService/handle_task_list_get
  - functions/crates/lpe-jmap/src/tasks/JmapService/handle_task_get
---

# Signature

`pub(crate) fn parse_uuid_list(value: Option<Vec<String>>) -> Result<Option<Vec<Uuid>>>`

# Calls

- [parse_uuid](../../../../../functions/crates/lpe-jmap/src/parse/parse_uuid.md)

# Called by

- [handle_calendar_event_get](../../../../../functions/crates/lpe-jmap/src/calendar/JmapService/handle_calendar_event_get.md)
- [handle_contact_get](../../../../../functions/crates/lpe-jmap/src/contacts/JmapService/handle_contact_get.md)
- [handle_email_get](../../../../../functions/crates/lpe-jmap/src/mail/JmapService/handle_email_get.md)
- [handle_email_submission_get](../../../../../functions/crates/lpe-jmap/src/mail/JmapService/handle_email_submission_get.md)
- [handle_search_snippet_get](../../../../../functions/crates/lpe-jmap/src/mail/JmapService/handle_search_snippet_get.md)
- [handle_mailbox_get](../../../../../functions/crates/lpe-jmap/src/mailboxes/JmapService/handle_mailbox_get.md)
- [handle_note_get](../../../../../functions/crates/lpe-jmap/src/notes_journal/JmapService/handle_note_get.md)
- [handle_journal_entry_get](../../../../../functions/crates/lpe-jmap/src/notes_journal/JmapService/handle_journal_entry_get.md)
- [handle_task_list_get](../../../../../functions/crates/lpe-jmap/src/tasks/JmapService/handle_task_list_get.md)
- [handle_task_get](../../../../../functions/crates/lpe-jmap/src/tasks/JmapService/handle_task_get.md)