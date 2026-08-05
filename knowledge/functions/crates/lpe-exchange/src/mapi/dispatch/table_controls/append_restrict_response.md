---
type: Rust Function
title: append_restrict_response
resource: crates/lpe-exchange/src/mapi/dispatch/table_controls.rs#L716-L850
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_validation/table_async_flags_are_valid
  - functions/crates/lpe-exchange/src/mapi/session/input_object_mut
  - functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index
  - functions/crates/lpe-exchange/src/mapi/session/input_object
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/restriction
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/named_properties/format_contents_table_named_property_context
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/table_queries/log_outlook_contents_table_restrict
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/restrict_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_lifecycle/record_normal_inbox_table_lifecycle
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/input_handle_index
  - functions/crates/lpe-exchange/src/mapi/session/input_handle
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_outlook_view_failure_trace_event
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_restrict_table_control_response
---

# Signature

`pub(super) fn append_restrict_response( principal: &AccountPrincipal, session: &mut MapiSession, handle_slots: &[u32], request: &RopRequest, request_id: &str, request_rop_names: &str, snapshot: &MapiMailStoreSnapshot, responses: &mut Vec<u8>, ) -> TableControlFlow`

# Calls

- [table_async_flags_are_valid](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_validation/table_async_flags_are_valid.md)
- [input_object_mut](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/input_object_mut.md)
- [rop_error_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response.md)
- [response_handle_index](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index.md)
- [input_object](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/input_object.md)
- [restriction](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/restriction.md)
- [format_contents_table_named_property_context](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/named_properties/format_contents_table_named_property_context.md)
- [log_outlook_contents_table_restrict](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/table_queries/log_outlook_contents_table_restrict.md)
- [restrict_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/restrict_response.md)
- [record_normal_inbox_table_lifecycle](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_lifecycle/record_normal_inbox_table_lifecycle.md)
- [input_handle_index](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/input_handle_index.md)
- [input_handle](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/input_handle.md)
- [record_outlook_view_failure_trace_event](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_outlook_view_failure_trace_event.md)

# Called by

- [append_restrict_table_control_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_restrict_table_control_response.md)