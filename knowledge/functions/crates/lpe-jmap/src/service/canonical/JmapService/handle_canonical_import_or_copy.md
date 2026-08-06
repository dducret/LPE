---
type: Rust Method
title: handle_canonical_import_or_copy
resource: crates/lpe-jmap/src/service/canonical.rs#L347-L401
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-jmap/src/contacts/JmapService/handle_contact_set
  - functions/crates/lpe-jmap/src/calendar/JmapService/handle_calendar_event_set
  - functions/crates/lpe-jmap/src/tasks/JmapService/handle_task_list_set
  - functions/crates/lpe-jmap/src/tasks/JmapService/handle_task_set
  - functions/crates/lpe-jmap/src/notes_journal/JmapService/handle_note_set
  - functions/crates/lpe-jmap/src/notes_journal/JmapService/handle_journal_entry_set
  - functions/crates/lpe-jmap/src/service/canonical/JmapService/handle_canonical_unsupported_write
  called_by:
  - functions/crates/lpe-jmap/src/service/JmapService/handle_api_request_for_account
---

# Signature

`pub(crate) async fn handle_canonical_import_or_copy( &self, account: &AuthenticatedAccount, arguments: Value, created_ids: &mut HashMap<String, String>, data_type: &str, method_name: &str, ) -> Result<Value>`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [handle_contact_set](../../../../../../../functions/crates/lpe-jmap/src/contacts/JmapService/handle_contact_set.md)
- [handle_calendar_event_set](../../../../../../../functions/crates/lpe-jmap/src/calendar/JmapService/handle_calendar_event_set.md)
- [handle_task_list_set](../../../../../../../functions/crates/lpe-jmap/src/tasks/JmapService/handle_task_list_set.md)
- [handle_task_set](../../../../../../../functions/crates/lpe-jmap/src/tasks/JmapService/handle_task_set.md)
- [handle_note_set](../../../../../../../functions/crates/lpe-jmap/src/notes_journal/JmapService/handle_note_set.md)
- [handle_journal_entry_set](../../../../../../../functions/crates/lpe-jmap/src/notes_journal/JmapService/handle_journal_entry_set.md)
- [handle_canonical_unsupported_write](../../../../../../../functions/crates/lpe-jmap/src/service/canonical/JmapService/handle_canonical_unsupported_write.md)

# Called by

- [handle_api_request_for_account](../../../../../../../functions/crates/lpe-jmap/src/service/JmapService/handle_api_request_for_account.md)