---
type: Rust Function
title: set_error
resource: crates/lpe-jmap/src/error.rs#L69-L71
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/error/method_error
  called_by:
  - functions/crates/lpe-jmap/src/blob/JmapService/handle_blob_upload
  - functions/crates/lpe-jmap/src/blob/JmapService/handle_blob_copy
  - functions/crates/lpe-jmap/src/calendar/JmapService/handle_calendar_set
  - functions/crates/lpe-jmap/src/calendar/JmapService/handle_calendar_event_set
  - functions/crates/lpe-jmap/src/contacts/JmapService/handle_contact_set
  - functions/crates/lpe-jmap/src/mail/JmapService/handle_email_copy
  - functions/crates/lpe-jmap/src/mail/JmapService/handle_email_import
  - functions/crates/lpe-jmap/src/mail/JmapService/handle_email_set
  - functions/crates/lpe-jmap/src/mail/JmapService/handle_email_submission_set
  - functions/crates/lpe-jmap/src/mailboxes/JmapService/handle_mailbox_set
  - functions/crates/lpe-jmap/src/notes_journal/JmapService/handle_note_set
  - functions/crates/lpe-jmap/src/notes_journal/JmapService/handle_journal_entry_set
  - functions/crates/lpe-jmap/src/service/JmapService/handle_reminder_set
  - functions/crates/lpe-jmap/src/service/JmapService/handle_share_set
  - functions/crates/lpe-jmap/src/service/JmapService/handle_search_folder_set
  - functions/crates/lpe-jmap/src/tasks/JmapService/handle_task_list_set
  - functions/crates/lpe-jmap/src/tasks/JmapService/handle_task_set
  - functions/crates/lpe-jmap/src/vacation/JmapService/handle_vacation_response_set
---

# Signature

`pub(crate) fn set_error(description: &str) -> Value`

# Calls

- [method_error](../../../../../functions/crates/lpe-jmap/src/error/method_error.md)

# Called by

- [handle_blob_upload](../../../../../functions/crates/lpe-jmap/src/blob/JmapService/handle_blob_upload.md)
- [handle_blob_copy](../../../../../functions/crates/lpe-jmap/src/blob/JmapService/handle_blob_copy.md)
- [handle_calendar_set](../../../../../functions/crates/lpe-jmap/src/calendar/JmapService/handle_calendar_set.md)
- [handle_calendar_event_set](../../../../../functions/crates/lpe-jmap/src/calendar/JmapService/handle_calendar_event_set.md)
- [handle_contact_set](../../../../../functions/crates/lpe-jmap/src/contacts/JmapService/handle_contact_set.md)
- [handle_email_copy](../../../../../functions/crates/lpe-jmap/src/mail/JmapService/handle_email_copy.md)
- [handle_email_import](../../../../../functions/crates/lpe-jmap/src/mail/JmapService/handle_email_import.md)
- [handle_email_set](../../../../../functions/crates/lpe-jmap/src/mail/JmapService/handle_email_set.md)
- [handle_email_submission_set](../../../../../functions/crates/lpe-jmap/src/mail/JmapService/handle_email_submission_set.md)
- [handle_mailbox_set](../../../../../functions/crates/lpe-jmap/src/mailboxes/JmapService/handle_mailbox_set.md)
- [handle_note_set](../../../../../functions/crates/lpe-jmap/src/notes_journal/JmapService/handle_note_set.md)
- [handle_journal_entry_set](../../../../../functions/crates/lpe-jmap/src/notes_journal/JmapService/handle_journal_entry_set.md)
- [handle_reminder_set](../../../../../functions/crates/lpe-jmap/src/service/JmapService/handle_reminder_set.md)
- [handle_share_set](../../../../../functions/crates/lpe-jmap/src/service/JmapService/handle_share_set.md)
- [handle_search_folder_set](../../../../../functions/crates/lpe-jmap/src/service/JmapService/handle_search_folder_set.md)
- [handle_task_list_set](../../../../../functions/crates/lpe-jmap/src/tasks/JmapService/handle_task_list_set.md)
- [handle_task_set](../../../../../functions/crates/lpe-jmap/src/tasks/JmapService/handle_task_set.md)
- [handle_vacation_response_set](../../../../../functions/crates/lpe-jmap/src/vacation/JmapService/handle_vacation_response_set.md)