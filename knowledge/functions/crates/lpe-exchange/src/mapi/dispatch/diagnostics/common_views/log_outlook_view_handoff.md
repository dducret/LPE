---
type: Rust Function
title: log_outlook_view_handoff
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views.rs#L56-L149
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/views/outlook_folder_view_definition
  - functions/crates/lpe-exchange/src/mapi/properties/views/view_descriptor_binary
  - functions/crates/lpe-exchange/src/mapi/properties/views/view_descriptor_strings
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/format_view_descriptor_binary_summary
  - functions/crates/lpe-exchange/src/mapi/properties/views/outlook_view_descriptor_clsid
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/debug_default_folder_associated_named_view
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/format_view_handoff_invariant_warnings
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/message_open/append_open_message_response
---

# Signature

`pub(in crate::mapi::dispatch) fn log_outlook_view_handoff( principal: &AccountPrincipal, request: &RopRequest, folder_id: u64, message_id: u64, output_handle: u32, message: &crate::mapi_store::MapiCommonViewNamedViewMessage, snapshot: &MapiMailStoreSnapshot, )`

# Calls

- [outlook_folder_view_definition](../../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/views/outlook_folder_view_definition.md)
- [view_descriptor_binary](../../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/views/view_descriptor_binary.md)
- [view_descriptor_strings](../../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/views/view_descriptor_strings.md)
- [format_view_descriptor_binary_summary](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/format_view_descriptor_binary_summary.md)
- [outlook_view_descriptor_clsid](../../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/views/outlook_view_descriptor_clsid.md)
- [debug_default_folder_associated_named_view](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/debug_default_folder_associated_named_view.md)
- [format_view_handoff_invariant_warnings](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/format_view_handoff_invariant_warnings.md)

# Called by

- [append_open_message_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/message_open/append_open_message_response.md)