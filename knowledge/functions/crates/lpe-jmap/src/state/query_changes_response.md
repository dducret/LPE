---
type: Rust Function
title: query_changes_response
resource: crates/lpe-jmap/src/state.rs#L491-L522
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/state/decode_query_state
  - functions/crates/lpe-jmap/src/state/query_diff_for_kind
  - functions/crates/lpe-jmap/src/state/encode_query_state
  - functions/crates/lpe-jmap/src/state/query_changes_response_from_diff
  called_by:
  - functions/crates/lpe-jmap/src/calendar/JmapService/handle_calendar_query_changes
  - functions/crates/lpe-jmap/src/calendar/JmapService/handle_calendar_event_query_changes
  - functions/crates/lpe-jmap/src/contacts/JmapService/handle_address_book_query_changes
  - functions/crates/lpe-jmap/src/contacts/JmapService/handle_contact_query_changes
  - functions/crates/lpe-jmap/src/mail/JmapService/handle_email_submission_query_changes
  - functions/crates/lpe-jmap/src/mail/JmapService/handle_thread_query_changes
  - functions/crates/lpe-jmap/src/notes_journal/JmapService/handle_note_query_changes
  - functions/crates/lpe-jmap/src/notes_journal/JmapService/handle_journal_entry_query_changes
  - functions/crates/lpe-jmap/src/state/query_changes_response_returns_intermediate_query_state_when_truncated
  - functions/crates/lpe-jmap/src/state/email_query_changes_reports_reorders_and_paginates_to_current_order
  - functions/crates/lpe-jmap/src/tasks/JmapService/handle_task_query_changes
---

# Signature

`pub(crate) fn query_changes_response( account_id: Uuid, kind: &str, since_query_state: String, filter: Option<Value>, sort: Option<Vec<Value>>, current_ids: Vec<String>, total: u64, max_changes: Option<u64>, ) -> Result<Value>`

# Calls

- [decode_query_state](../../../../../functions/crates/lpe-jmap/src/state/decode_query_state.md)
- [query_diff_for_kind](../../../../../functions/crates/lpe-jmap/src/state/query_diff_for_kind.md)
- [encode_query_state](../../../../../functions/crates/lpe-jmap/src/state/encode_query_state.md)
- [query_changes_response_from_diff](../../../../../functions/crates/lpe-jmap/src/state/query_changes_response_from_diff.md)

# Called by

- [handle_calendar_query_changes](../../../../../functions/crates/lpe-jmap/src/calendar/JmapService/handle_calendar_query_changes.md)
- [handle_calendar_event_query_changes](../../../../../functions/crates/lpe-jmap/src/calendar/JmapService/handle_calendar_event_query_changes.md)
- [handle_address_book_query_changes](../../../../../functions/crates/lpe-jmap/src/contacts/JmapService/handle_address_book_query_changes.md)
- [handle_contact_query_changes](../../../../../functions/crates/lpe-jmap/src/contacts/JmapService/handle_contact_query_changes.md)
- [handle_email_submission_query_changes](../../../../../functions/crates/lpe-jmap/src/mail/JmapService/handle_email_submission_query_changes.md)
- [handle_thread_query_changes](../../../../../functions/crates/lpe-jmap/src/mail/JmapService/handle_thread_query_changes.md)
- [handle_note_query_changes](../../../../../functions/crates/lpe-jmap/src/notes_journal/JmapService/handle_note_query_changes.md)
- [handle_journal_entry_query_changes](../../../../../functions/crates/lpe-jmap/src/notes_journal/JmapService/handle_journal_entry_query_changes.md)
- [query_changes_response_returns_intermediate_query_state_when_truncated](../../../../../functions/crates/lpe-jmap/src/state/query_changes_response_returns_intermediate_query_state_when_truncated.md)
- [email_query_changes_reports_reorders_and_paginates_to_current_order](../../../../../functions/crates/lpe-jmap/src/state/email_query_changes_reports_reorders_and_paginates_to_current_order.md)
- [handle_task_query_changes](../../../../../functions/crates/lpe-jmap/src/tasks/JmapService/handle_task_query_changes.md)