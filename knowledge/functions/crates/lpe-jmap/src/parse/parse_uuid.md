---
type: Rust Function
title: parse_uuid
resource: crates/lpe-jmap/src/parse.rs#L9-L11
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-jmap/src/calendar/JmapService/handle_calendar_event_set
  - functions/crates/lpe-jmap/src/contacts/JmapService/handle_contact_set
  - functions/crates/lpe-jmap/src/drafts/parse_email_copy
  - functions/crates/lpe-jmap/src/mail/JmapService/handle_email_query
  - functions/crates/lpe-jmap/src/mail/JmapService/handle_email_set
  - functions/crates/lpe-jmap/src/mail/JmapService/handle_email_submission_set
  - functions/crates/lpe-jmap/src/mail/JmapService/handle_thread_get
  - functions/crates/lpe-jmap/src/mail/JmapService/update_draft
  - functions/crates/lpe-jmap/src/mail/imports/JmapService/parse_email_import
  - functions/crates/lpe-jmap/src/mailboxes/JmapService/handle_mailbox_set
  - functions/crates/lpe-jmap/src/mailboxes/parse_parent_id_field
  - functions/crates/lpe-jmap/src/mailboxes/validate_mailbox_set_names
  - functions/crates/lpe-jmap/src/notes_journal/JmapService/handle_note_set
  - functions/crates/lpe-jmap/src/notes_journal/JmapService/handle_journal_entry_set
  - functions/crates/lpe-jmap/src/parse/parse_uuid_list
  - functions/crates/lpe-jmap/src/service/JmapService/requested_account_access
  - functions/crates/lpe-jmap/src/service/JmapService/handle_search_folder_set
  - functions/crates/lpe-jmap/src/service/helpers/parse_reminder_id
  - functions/crates/lpe-jmap/src/session/requested_account_id
  - functions/crates/lpe-jmap/src/tasks/JmapService/handle_task_list_set
  - functions/crates/lpe-jmap/src/tasks/JmapService/handle_task_set
  - functions/crates/lpe-jmap/src/tasks/validate_task_list_id
  - functions/crates/lpe-jmap/src/upload/JmapBlobId/parse
  - functions/crates/lpe-jmap/src/validation/validate_task_filter
---

# Signature

`pub(crate) fn parse_uuid(value: &str) -> Result<Uuid>`

# Called by

- [handle_calendar_event_set](../../../../../functions/crates/lpe-jmap/src/calendar/JmapService/handle_calendar_event_set.md)
- [handle_contact_set](../../../../../functions/crates/lpe-jmap/src/contacts/JmapService/handle_contact_set.md)
- [parse_email_copy](../../../../../functions/crates/lpe-jmap/src/drafts/parse_email_copy.md)
- [handle_email_query](../../../../../functions/crates/lpe-jmap/src/mail/JmapService/handle_email_query.md)
- [handle_email_set](../../../../../functions/crates/lpe-jmap/src/mail/JmapService/handle_email_set.md)
- [handle_email_submission_set](../../../../../functions/crates/lpe-jmap/src/mail/JmapService/handle_email_submission_set.md)
- [handle_thread_get](../../../../../functions/crates/lpe-jmap/src/mail/JmapService/handle_thread_get.md)
- [update_draft](../../../../../functions/crates/lpe-jmap/src/mail/JmapService/update_draft.md)
- [parse_email_import](../../../../../functions/crates/lpe-jmap/src/mail/imports/JmapService/parse_email_import.md)
- [handle_mailbox_set](../../../../../functions/crates/lpe-jmap/src/mailboxes/JmapService/handle_mailbox_set.md)
- [parse_parent_id_field](../../../../../functions/crates/lpe-jmap/src/mailboxes/parse_parent_id_field.md)
- [validate_mailbox_set_names](../../../../../functions/crates/lpe-jmap/src/mailboxes/validate_mailbox_set_names.md)
- [handle_note_set](../../../../../functions/crates/lpe-jmap/src/notes_journal/JmapService/handle_note_set.md)
- [handle_journal_entry_set](../../../../../functions/crates/lpe-jmap/src/notes_journal/JmapService/handle_journal_entry_set.md)
- [parse_uuid_list](../../../../../functions/crates/lpe-jmap/src/parse/parse_uuid_list.md)
- [requested_account_access](../../../../../functions/crates/lpe-jmap/src/service/JmapService/requested_account_access.md)
- [handle_search_folder_set](../../../../../functions/crates/lpe-jmap/src/service/JmapService/handle_search_folder_set.md)
- [parse_reminder_id](../../../../../functions/crates/lpe-jmap/src/service/helpers/parse_reminder_id.md)
- [requested_account_id](../../../../../functions/crates/lpe-jmap/src/session/requested_account_id.md)
- [handle_task_list_set](../../../../../functions/crates/lpe-jmap/src/tasks/JmapService/handle_task_list_set.md)
- [handle_task_set](../../../../../functions/crates/lpe-jmap/src/tasks/JmapService/handle_task_set.md)
- [validate_task_list_id](../../../../../functions/crates/lpe-jmap/src/tasks/validate_task_list_id.md)
- [parse](../../../../../functions/crates/lpe-jmap/src/upload/JmapBlobId/parse.md)
- [validate_task_filter](../../../../../functions/crates/lpe-jmap/src/validation/validate_task_filter.md)