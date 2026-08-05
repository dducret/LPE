---
type: Rust Method
title: record_completed_sync_checkpoint
resource: crates/lpe-exchange/src/mapi/session.rs#L67-L105
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_get_buffer/append_fast_transfer_source_get_buffer_response
  - functions/crates/lpe-exchange/src/mapi/session/tests/session_records_completed_sync_checkpoint_once
  - functions/crates/lpe-exchange/src/mapi/transport/tests/partial_scope_checkpoint_not_stored_count_counts_expected_partial_scope_summaries
  - functions/crates/lpe-exchange/src/mapi/transport/tests/required_default_folder_disconnect_coverage_reports_calendar_contacts_gap
---

# Signature

`pub(in crate::mapi) fn record_completed_sync_checkpoint( &mut self, folder_id: u64, folder_role: &str, folder_container_class: &str, checkpoint_kind: &str, sync_type: u8, status: &str, )`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [append_fast_transfer_source_get_buffer_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_get_buffer/append_fast_transfer_source_get_buffer_response.md)
- [session_records_completed_sync_checkpoint_once](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/tests/session_records_completed_sync_checkpoint_once.md)
- [partial_scope_checkpoint_not_stored_count_counts_expected_partial_scope_summaries](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/tests/partial_scope_checkpoint_not_stored_count_counts_expected_partial_scope_summaries.md)
- [required_default_folder_disconnect_coverage_reports_calendar_contacts_gap](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/tests/required_default_folder_disconnect_coverage_reports_calendar_contacts_gap.md)