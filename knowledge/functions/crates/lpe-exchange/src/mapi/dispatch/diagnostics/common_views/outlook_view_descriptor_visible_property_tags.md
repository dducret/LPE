---
type: Rust Function
title: outlook_view_descriptor_visible_property_tags
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views.rs#L333-L343
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/debug_advertised_default_named_view
  - functions/crates/lpe-exchange/src/mapi/properties/views/outlook_folder_view_definition
  - functions/crates/lpe-exchange/src/mapi/properties/views/view_descriptor_binary
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/view_descriptor_runtime_property_tags
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/table_queries/log_mapi_query_position_debug
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/append_table_control_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/outlook_view_descriptor_visible_property_tags_are_empty_for_calendar_normal_view
---

# Signature

`pub(in crate::mapi::dispatch) fn outlook_view_descriptor_visible_property_tags( folder_id: u64, snapshot: &MapiMailStoreSnapshot, ) -> Vec<u32>`

# Calls

- [debug_advertised_default_named_view](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/debug_advertised_default_named_view.md)
- [outlook_folder_view_definition](../../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/views/outlook_folder_view_definition.md)
- [view_descriptor_binary](../../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/views/view_descriptor_binary.md)
- [view_descriptor_runtime_property_tags](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/view_descriptor_runtime_property_tags.md)

# Called by

- [log_mapi_query_position_debug](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/table_queries/log_mapi_query_position_debug.md)
- [append_table_control_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/append_table_control_response.md)
- [outlook_view_descriptor_visible_property_tags_are_empty_for_calendar_normal_view](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/outlook_view_descriptor_visible_property_tags_are_empty_for_calendar_normal_view.md)