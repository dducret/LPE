---
type: Rust Function
title: format_inbox_view_descriptor_set_columns_behavior_contract
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views.rs#L428-L464
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/debug_advertised_default_named_view
  - functions/crates/lpe-exchange/src/mapi/properties/views/outlook_folder_view_definition
  - functions/crates/lpe-exchange/src/mapi/properties/views/view_descriptor_binary
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/view_descriptor_runtime_property_tags
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/view_descriptor_comparable_selected_columns
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/missing_debug_property_tags
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/default_view_setcolumns_projection_kind
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/table_queries/log_outlook_contents_table_set_columns
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_set_columns_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/inbox_view_descriptor_set_columns_contract_requires_persisted_view
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/inbox_compact_descriptor_is_missing_without_persisted_view
---

# Signature

`pub(in crate::mapi::dispatch) fn format_inbox_view_descriptor_set_columns_behavior_contract( folder_id: u64, associated: bool, columns: &[u32], snapshot: &MapiMailStoreSnapshot, ) -> String`

# Calls

- [debug_advertised_default_named_view](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/debug_advertised_default_named_view.md)
- [outlook_folder_view_definition](../../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/views/outlook_folder_view_definition.md)
- [view_descriptor_binary](../../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/views/view_descriptor_binary.md)
- [view_descriptor_runtime_property_tags](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/view_descriptor_runtime_property_tags.md)
- [view_descriptor_comparable_selected_columns](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/view_descriptor_comparable_selected_columns.md)
- [missing_debug_property_tags](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/missing_debug_property_tags.md)
- [default_view_setcolumns_projection_kind](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/default_view_setcolumns_projection_kind.md)

# Called by

- [log_outlook_contents_table_set_columns](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/table_queries/log_outlook_contents_table_set_columns.md)
- [append_set_columns_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_set_columns_response.md)
- [inbox_view_descriptor_set_columns_contract_requires_persisted_view](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/inbox_view_descriptor_set_columns_contract_requires_persisted_view.md)
- [inbox_compact_descriptor_is_missing_without_persisted_view](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/inbox_compact_descriptor_is_missing_without_persisted_view.md)