---
type: Rust Function
title: requested_account_id
resource: crates/lpe-jmap/src/session.rs#L217-L232
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/parse/parse_uuid
  called_by:
  - functions/crates/lpe-jmap/src/calendar/JmapService/handle_calendar_get
  - functions/crates/lpe-jmap/src/calendar/JmapService/handle_calendar_query
  - functions/crates/lpe-jmap/src/calendar/JmapService/handle_calendar_query_changes
  - functions/crates/lpe-jmap/src/calendar/JmapService/handle_calendar_changes
  - functions/crates/lpe-jmap/src/calendar/JmapService/handle_calendar_set
  - functions/crates/lpe-jmap/src/calendar/JmapService/handle_calendar_event_get
  - functions/crates/lpe-jmap/src/calendar/JmapService/handle_calendar_event_query
  - functions/crates/lpe-jmap/src/calendar/JmapService/handle_calendar_event_query_changes
  - functions/crates/lpe-jmap/src/calendar/JmapService/handle_calendar_event_changes
  - functions/crates/lpe-jmap/src/calendar/JmapService/handle_calendar_event_set
  - functions/crates/lpe-jmap/src/contacts/JmapService/handle_address_book_get
  - functions/crates/lpe-jmap/src/contacts/JmapService/handle_address_book_query
  - functions/crates/lpe-jmap/src/contacts/JmapService/handle_address_book_query_changes
  - functions/crates/lpe-jmap/src/contacts/JmapService/handle_address_book_changes
  - functions/crates/lpe-jmap/src/contacts/JmapService/handle_contact_get
  - functions/crates/lpe-jmap/src/contacts/JmapService/handle_contact_query
  - functions/crates/lpe-jmap/src/contacts/JmapService/handle_contact_query_changes
  - functions/crates/lpe-jmap/src/contacts/JmapService/handle_contact_changes
  - functions/crates/lpe-jmap/src/contacts/JmapService/handle_contact_set
  - functions/crates/lpe-jmap/src/contacts/JmapService/handle_recipient_suggestion_query
  - functions/crates/lpe-jmap/src/notes_journal/JmapService/handle_note_get
  - functions/crates/lpe-jmap/src/notes_journal/JmapService/handle_note_query
  - functions/crates/lpe-jmap/src/notes_journal/JmapService/handle_note_query_changes
  - functions/crates/lpe-jmap/src/notes_journal/JmapService/handle_note_changes
  - functions/crates/lpe-jmap/src/notes_journal/JmapService/handle_note_set
  - functions/crates/lpe-jmap/src/notes_journal/JmapService/handle_journal_entry_get
  - functions/crates/lpe-jmap/src/notes_journal/JmapService/handle_journal_entry_query
  - functions/crates/lpe-jmap/src/notes_journal/JmapService/handle_journal_entry_query_changes
  - functions/crates/lpe-jmap/src/notes_journal/JmapService/handle_journal_entry_changes
  - functions/crates/lpe-jmap/src/notes_journal/JmapService/handle_journal_entry_set
  - functions/crates/lpe-jmap/src/notes_journal/JmapService/handle_reminder_query
  - functions/crates/lpe-jmap/src/service/helpers/requested_account_id_from_arguments
  - functions/crates/lpe-jmap/src/tasks/JmapService/handle_task_list_get
  - functions/crates/lpe-jmap/src/tasks/JmapService/handle_task_list_changes
  - functions/crates/lpe-jmap/src/tasks/JmapService/handle_task_list_set
  - functions/crates/lpe-jmap/src/tasks/JmapService/handle_task_get
  - functions/crates/lpe-jmap/src/tasks/JmapService/handle_task_query
  - functions/crates/lpe-jmap/src/tasks/JmapService/handle_task_query_changes
  - functions/crates/lpe-jmap/src/tasks/JmapService/handle_task_changes
  - functions/crates/lpe-jmap/src/tasks/JmapService/handle_task_set
  - functions/crates/lpe-jmap/src/vacation/JmapService/handle_vacation_response_get
  - functions/crates/lpe-jmap/src/vacation/JmapService/handle_vacation_response_set
---

# Signature

`pub(crate) fn requested_account_id( requested_account_id: Option<&str>, account: &AuthenticatedAccount, ) -> Result<Uuid>`

# Calls

- [parse_uuid](../../../../../functions/crates/lpe-jmap/src/parse/parse_uuid.md)

# Called by

- [handle_calendar_get](../../../../../functions/crates/lpe-jmap/src/calendar/JmapService/handle_calendar_get.md)
- [handle_calendar_query](../../../../../functions/crates/lpe-jmap/src/calendar/JmapService/handle_calendar_query.md)
- [handle_calendar_query_changes](../../../../../functions/crates/lpe-jmap/src/calendar/JmapService/handle_calendar_query_changes.md)
- [handle_calendar_changes](../../../../../functions/crates/lpe-jmap/src/calendar/JmapService/handle_calendar_changes.md)
- [handle_calendar_set](../../../../../functions/crates/lpe-jmap/src/calendar/JmapService/handle_calendar_set.md)
- [handle_calendar_event_get](../../../../../functions/crates/lpe-jmap/src/calendar/JmapService/handle_calendar_event_get.md)
- [handle_calendar_event_query](../../../../../functions/crates/lpe-jmap/src/calendar/JmapService/handle_calendar_event_query.md)
- [handle_calendar_event_query_changes](../../../../../functions/crates/lpe-jmap/src/calendar/JmapService/handle_calendar_event_query_changes.md)
- [handle_calendar_event_changes](../../../../../functions/crates/lpe-jmap/src/calendar/JmapService/handle_calendar_event_changes.md)
- [handle_calendar_event_set](../../../../../functions/crates/lpe-jmap/src/calendar/JmapService/handle_calendar_event_set.md)
- [handle_address_book_get](../../../../../functions/crates/lpe-jmap/src/contacts/JmapService/handle_address_book_get.md)
- [handle_address_book_query](../../../../../functions/crates/lpe-jmap/src/contacts/JmapService/handle_address_book_query.md)
- [handle_address_book_query_changes](../../../../../functions/crates/lpe-jmap/src/contacts/JmapService/handle_address_book_query_changes.md)
- [handle_address_book_changes](../../../../../functions/crates/lpe-jmap/src/contacts/JmapService/handle_address_book_changes.md)
- [handle_contact_get](../../../../../functions/crates/lpe-jmap/src/contacts/JmapService/handle_contact_get.md)
- [handle_contact_query](../../../../../functions/crates/lpe-jmap/src/contacts/JmapService/handle_contact_query.md)
- [handle_contact_query_changes](../../../../../functions/crates/lpe-jmap/src/contacts/JmapService/handle_contact_query_changes.md)
- [handle_contact_changes](../../../../../functions/crates/lpe-jmap/src/contacts/JmapService/handle_contact_changes.md)
- [handle_contact_set](../../../../../functions/crates/lpe-jmap/src/contacts/JmapService/handle_contact_set.md)
- [handle_recipient_suggestion_query](../../../../../functions/crates/lpe-jmap/src/contacts/JmapService/handle_recipient_suggestion_query.md)
- [handle_note_get](../../../../../functions/crates/lpe-jmap/src/notes_journal/JmapService/handle_note_get.md)
- [handle_note_query](../../../../../functions/crates/lpe-jmap/src/notes_journal/JmapService/handle_note_query.md)
- [handle_note_query_changes](../../../../../functions/crates/lpe-jmap/src/notes_journal/JmapService/handle_note_query_changes.md)
- [handle_note_changes](../../../../../functions/crates/lpe-jmap/src/notes_journal/JmapService/handle_note_changes.md)
- [handle_note_set](../../../../../functions/crates/lpe-jmap/src/notes_journal/JmapService/handle_note_set.md)
- [handle_journal_entry_get](../../../../../functions/crates/lpe-jmap/src/notes_journal/JmapService/handle_journal_entry_get.md)
- [handle_journal_entry_query](../../../../../functions/crates/lpe-jmap/src/notes_journal/JmapService/handle_journal_entry_query.md)
- [handle_journal_entry_query_changes](../../../../../functions/crates/lpe-jmap/src/notes_journal/JmapService/handle_journal_entry_query_changes.md)
- [handle_journal_entry_changes](../../../../../functions/crates/lpe-jmap/src/notes_journal/JmapService/handle_journal_entry_changes.md)
- [handle_journal_entry_set](../../../../../functions/crates/lpe-jmap/src/notes_journal/JmapService/handle_journal_entry_set.md)
- [handle_reminder_query](../../../../../functions/crates/lpe-jmap/src/notes_journal/JmapService/handle_reminder_query.md)
- [requested_account_id_from_arguments](../../../../../functions/crates/lpe-jmap/src/service/helpers/requested_account_id_from_arguments.md)
- [handle_task_list_get](../../../../../functions/crates/lpe-jmap/src/tasks/JmapService/handle_task_list_get.md)
- [handle_task_list_changes](../../../../../functions/crates/lpe-jmap/src/tasks/JmapService/handle_task_list_changes.md)
- [handle_task_list_set](../../../../../functions/crates/lpe-jmap/src/tasks/JmapService/handle_task_list_set.md)
- [handle_task_get](../../../../../functions/crates/lpe-jmap/src/tasks/JmapService/handle_task_get.md)
- [handle_task_query](../../../../../functions/crates/lpe-jmap/src/tasks/JmapService/handle_task_query.md)
- [handle_task_query_changes](../../../../../functions/crates/lpe-jmap/src/tasks/JmapService/handle_task_query_changes.md)
- [handle_task_changes](../../../../../functions/crates/lpe-jmap/src/tasks/JmapService/handle_task_changes.md)
- [handle_task_set](../../../../../functions/crates/lpe-jmap/src/tasks/JmapService/handle_task_set.md)
- [handle_vacation_response_get](../../../../../functions/crates/lpe-jmap/src/vacation/JmapService/handle_vacation_response_get.md)
- [handle_vacation_response_set](../../../../../functions/crates/lpe-jmap/src/vacation/JmapService/handle_vacation_response_set.md)