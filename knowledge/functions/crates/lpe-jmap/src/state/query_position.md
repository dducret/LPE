---
type: Rust Function
title: query_position
resource: crates/lpe-jmap/src/state.rs#L593-L615
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/position
  - functions/crates/lpe-activesync/src/protocol/WbxmlCodePage/tryfrom-u8/try_from
  called_by:
  - functions/crates/lpe-jmap/src/calendar/JmapService/handle_calendar_query
  - functions/crates/lpe-jmap/src/calendar/JmapService/handle_calendar_event_query
  - functions/crates/lpe-jmap/src/contacts/JmapService/handle_address_book_query
  - functions/crates/lpe-jmap/src/contacts/JmapService/handle_contact_query
  - functions/crates/lpe-jmap/src/contacts/JmapService/handle_recipient_suggestion_query
  - functions/crates/lpe-jmap/src/mail/JmapService/handle_email_query
  - functions/crates/lpe-jmap/src/mail/JmapService/handle_email_submission_query
  - functions/crates/lpe-jmap/src/mail/JmapService/handle_thread_query
  - functions/crates/lpe-jmap/src/mailboxes/JmapService/handle_mailbox_query
  - functions/crates/lpe-jmap/src/notes_journal/JmapService/handle_note_query
  - functions/crates/lpe-jmap/src/notes_journal/JmapService/handle_journal_entry_query
  - functions/crates/lpe-jmap/src/notes_journal/JmapService/handle_reminder_query
  - functions/crates/lpe-jmap/src/service/canonical/JmapService/handle_canonical_query
  - functions/crates/lpe-jmap/src/tasks/JmapService/handle_task_query
---

# Signature

`pub(crate) fn query_position( ids: &[String], position: Option<i64>, anchor: Option<&str>, anchor_offset: Option<i64>, ) -> Result<usize>`

# Calls

- [position](../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/position.md)
- [try_from](../../../../../functions/crates/lpe-activesync/src/protocol/WbxmlCodePage/tryfrom-u8/try_from.md)

# Called by

- [handle_calendar_query](../../../../../functions/crates/lpe-jmap/src/calendar/JmapService/handle_calendar_query.md)
- [handle_calendar_event_query](../../../../../functions/crates/lpe-jmap/src/calendar/JmapService/handle_calendar_event_query.md)
- [handle_address_book_query](../../../../../functions/crates/lpe-jmap/src/contacts/JmapService/handle_address_book_query.md)
- [handle_contact_query](../../../../../functions/crates/lpe-jmap/src/contacts/JmapService/handle_contact_query.md)
- [handle_recipient_suggestion_query](../../../../../functions/crates/lpe-jmap/src/contacts/JmapService/handle_recipient_suggestion_query.md)
- [handle_email_query](../../../../../functions/crates/lpe-jmap/src/mail/JmapService/handle_email_query.md)
- [handle_email_submission_query](../../../../../functions/crates/lpe-jmap/src/mail/JmapService/handle_email_submission_query.md)
- [handle_thread_query](../../../../../functions/crates/lpe-jmap/src/mail/JmapService/handle_thread_query.md)
- [handle_mailbox_query](../../../../../functions/crates/lpe-jmap/src/mailboxes/JmapService/handle_mailbox_query.md)
- [handle_note_query](../../../../../functions/crates/lpe-jmap/src/notes_journal/JmapService/handle_note_query.md)
- [handle_journal_entry_query](../../../../../functions/crates/lpe-jmap/src/notes_journal/JmapService/handle_journal_entry_query.md)
- [handle_reminder_query](../../../../../functions/crates/lpe-jmap/src/notes_journal/JmapService/handle_reminder_query.md)
- [handle_canonical_query](../../../../../functions/crates/lpe-jmap/src/service/canonical/JmapService/handle_canonical_query.md)
- [handle_task_query](../../../../../functions/crates/lpe-jmap/src/tasks/JmapService/handle_task_query.md)