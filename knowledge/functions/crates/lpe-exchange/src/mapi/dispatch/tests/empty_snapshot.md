---
type: Rust Function
title: empty_snapshot
resource: crates/lpe-exchange/src/mapi/dispatch/tests.rs#L3111-L3124
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/normal_inbox_query_row_summary_reports_message_shapes
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/calendar_associated_sort_trace_reports_missing_query_rows_handoff
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/folders/calendar_folder_getprops_trace_summarizes_response_contract
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/folders/default_folder_hierarchy_projection_reports_calendar_and_contacts_identity
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/folders/root_default_folder_getprops_uses_canonical_projection_not_setprops_state
---

# Signature

`fn empty_snapshot() -> MapiMailStoreSnapshot`

# Called by

- [normal_inbox_query_row_summary_reports_message_shapes](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/normal_inbox_query_row_summary_reports_message_shapes.md)
- [calendar_associated_sort_trace_reports_missing_query_rows_handoff](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/calendar_associated_sort_trace_reports_missing_query_rows_handoff.md)
- [calendar_folder_getprops_trace_summarizes_response_contract](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/folders/calendar_folder_getprops_trace_summarizes_response_contract.md)
- [default_folder_hierarchy_projection_reports_calendar_and_contacts_identity](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/folders/default_folder_hierarchy_projection_reports_calendar_and_contacts_identity.md)
- [root_default_folder_getprops_uses_canonical_projection_not_setprops_state](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/folders/root_default_folder_getprops_uses_canonical_projection_not_setprops_state.md)