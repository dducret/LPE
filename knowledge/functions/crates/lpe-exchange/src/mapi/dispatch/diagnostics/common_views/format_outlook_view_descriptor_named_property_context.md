---
type: Rust Function
title: format_outlook_view_descriptor_named_property_context
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views.rs#L319-L331
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/debug_advertised_default_named_view
  - functions/crates/lpe-exchange/src/mapi/properties/views/outlook_folder_view_definition
  - functions/crates/lpe-exchange/src/mapi/properties/views/view_descriptor_binary
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/view_descriptor_runtime_property_tags
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/named_properties/format_debug_named_property_context
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_set_columns_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/outlook_view_descriptor_named_property_context_is_empty_without_persisted_view
---

# Signature

`pub(in crate::mapi::dispatch) fn format_outlook_view_descriptor_named_property_context( session: &MapiSession, folder_id: u64, snapshot: &MapiMailStoreSnapshot, ) -> String`

# Calls

- [debug_advertised_default_named_view](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/debug_advertised_default_named_view.md)
- [outlook_folder_view_definition](../../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/views/outlook_folder_view_definition.md)
- [view_descriptor_binary](../../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/views/view_descriptor_binary.md)
- [view_descriptor_runtime_property_tags](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/view_descriptor_runtime_property_tags.md)
- [format_debug_named_property_context](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/named_properties/format_debug_named_property_context.md)

# Called by

- [append_set_columns_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_set_columns_response.md)
- [outlook_view_descriptor_named_property_context_is_empty_without_persisted_view](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/outlook_view_descriptor_named_property_context_is_empty_without_persisted_view.md)