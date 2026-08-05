---
type: Rust Function
title: log_outlook_contents_table_find_row
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics/table_queries.rs#L101-L291
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/table_queries/rop_response_return_value
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/is_outlook_folder_table_debug_target
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/effective_contents_table_columns
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/restriction_property_tags_from_request
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_normal_message_find_row_failure_candidates
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/find_backward
  - functions/crates/lpe-exchange/src/mapi/tables/counts/associated_folder_message_count
  - functions/crates/lpe-exchange/src/mapi/tables/counts/folder_message_count
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_outlook_query_row_values_for_principal
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/associated_config/format_inbox_associated_wire_row_summary
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_normal_message_query_row_summary
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/format_outlook_view_handoff_table_contract
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/warn_outlook_view_handoff_table_invariants
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_find_row_response
---

# Signature

`pub(in crate::mapi::dispatch) fn log_outlook_contents_table_find_row( principal: &AccountPrincipal, request_id: &str, request: &RopRequest, object: Option<&MapiObject>, mailboxes: &[JmapMailbox], emails: &[JmapEmail], selected_named_property_context: &str, snapshot: &MapiMailStoreSnapshot, response: &[u8], )`

# Calls

- [rop_response_return_value](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/table_queries/rop_response_return_value.md)
- [is_outlook_folder_table_debug_target](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/is_outlook_folder_table_debug_target.md)
- [effective_contents_table_columns](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/effective_contents_table_columns.md)
- [restriction_property_tags_from_request](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/restriction_property_tags_from_request.md)
- [format_normal_message_find_row_failure_candidates](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_normal_message_find_row_failure_candidates.md)
- [find_backward](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/find_backward.md)
- [associated_folder_message_count](../../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/counts/associated_folder_message_count.md)
- [folder_message_count](../../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/counts/folder_message_count.md)
- [get](../../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [format_outlook_query_row_values_for_principal](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_outlook_query_row_values_for_principal.md)
- [format_inbox_associated_wire_row_summary](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/associated_config/format_inbox_associated_wire_row_summary.md)
- [format_normal_message_query_row_summary](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_normal_message_query_row_summary.md)
- [format_outlook_view_handoff_table_contract](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/format_outlook_view_handoff_table_contract.md)
- [warn_outlook_view_handoff_table_invariants](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/warn_outlook_view_handoff_table_invariants.md)

# Called by

- [append_find_row_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_find_row_response.md)