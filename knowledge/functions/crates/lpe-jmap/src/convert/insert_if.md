---
type: Rust Function
title: insert_if
resource: crates/lpe-jmap/src/convert.rs#L26-L38
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-jmap/src/calendar/calendar_to_value
  - functions/crates/lpe-jmap/src/calendar/calendar_event_to_value
  - functions/crates/lpe-jmap/src/contacts/address_book_to_value
  - functions/crates/lpe-jmap/src/contacts/contact_to_value
  - functions/crates/lpe-jmap/src/mail/values/email_to_value
  - functions/crates/lpe-jmap/src/mail/values/body_part_value
  - functions/crates/lpe-jmap/src/mail/values/email_submission_to_value
  - functions/crates/lpe-jmap/src/mail/values/identity_to_value
  - functions/crates/lpe-jmap/src/mail/values/thread_to_value
  - functions/crates/lpe-jmap/src/mailboxes/mailbox_to_value
  - functions/crates/lpe-jmap/src/notes_journal/note_to_value
  - functions/crates/lpe-jmap/src/notes_journal/journal_entry_to_value
  - functions/crates/lpe-jmap/src/tasks/task_list_to_value
  - functions/crates/lpe-jmap/src/tasks/task_to_value
  - functions/crates/lpe-jmap/src/vacation/vacation_response_to_value
---

# Signature

`pub(crate) fn insert_if<T: Serialize>( properties: &std::collections::HashSet<String>, object: &mut Map<String, Value>, key: &str, value: T, )`

# Called by

- [calendar_to_value](../../../../../functions/crates/lpe-jmap/src/calendar/calendar_to_value.md)
- [calendar_event_to_value](../../../../../functions/crates/lpe-jmap/src/calendar/calendar_event_to_value.md)
- [address_book_to_value](../../../../../functions/crates/lpe-jmap/src/contacts/address_book_to_value.md)
- [contact_to_value](../../../../../functions/crates/lpe-jmap/src/contacts/contact_to_value.md)
- [email_to_value](../../../../../functions/crates/lpe-jmap/src/mail/values/email_to_value.md)
- [body_part_value](../../../../../functions/crates/lpe-jmap/src/mail/values/body_part_value.md)
- [email_submission_to_value](../../../../../functions/crates/lpe-jmap/src/mail/values/email_submission_to_value.md)
- [identity_to_value](../../../../../functions/crates/lpe-jmap/src/mail/values/identity_to_value.md)
- [thread_to_value](../../../../../functions/crates/lpe-jmap/src/mail/values/thread_to_value.md)
- [mailbox_to_value](../../../../../functions/crates/lpe-jmap/src/mailboxes/mailbox_to_value.md)
- [note_to_value](../../../../../functions/crates/lpe-jmap/src/notes_journal/note_to_value.md)
- [journal_entry_to_value](../../../../../functions/crates/lpe-jmap/src/notes_journal/journal_entry_to_value.md)
- [task_list_to_value](../../../../../functions/crates/lpe-jmap/src/tasks/task_list_to_value.md)
- [task_to_value](../../../../../functions/crates/lpe-jmap/src/tasks/task_to_value.md)
- [vacation_response_to_value](../../../../../functions/crates/lpe-jmap/src/vacation/vacation_response_to_value.md)