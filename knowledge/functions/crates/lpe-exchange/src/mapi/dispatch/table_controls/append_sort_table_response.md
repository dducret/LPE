---
type: Rust Function
title: append_sort_table_response
resource: crates/lpe-exchange/src/mapi/dispatch/table_controls.rs#L582-L700
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/session/input_object
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/format_calendar_associated_sort_trace
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/format_optional_debug_handle
  - functions/crates/lpe-exchange/src/mapi/session/input_handle
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/sort_orders
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/normalize_table_property_tags_for_session
  - functions/crates/lpe-exchange/src/mapi/session/input_object_mut
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_validation/sort_table_request_is_valid
  - functions/crates/lpe-exchange/src/mapi/tables/state/invalid_table_sort_orders
  - functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_outlook_view_failure_trace_event
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/sort_category_count
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/sort_expanded_count
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/named_properties/format_contents_table_named_property_context
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/table_queries/log_outlook_contents_table_sort
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/sort_table_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_lifecycle/record_normal_inbox_table_lifecycle
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/input_handle_index
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_table_control_dispatch_response
---

# Signature

`pub(super) fn append_sort_table_response( principal: &AccountPrincipal, session: &mut MapiSession, handle_slots: &[u32], request: &RopRequest, request_id: &str, request_rop_names: &str, snapshot: &MapiMailStoreSnapshot, responses: &mut Vec<u8>, )`

# Calls

- [input_object](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/input_object.md)
- [format_calendar_associated_sort_trace](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/format_calendar_associated_sort_trace.md)
- [format_optional_debug_handle](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/format_optional_debug_handle.md)
- [input_handle](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/input_handle.md)
- [sort_orders](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/sort_orders.md)
- [normalize_table_property_tags_for_session](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/normalize_table_property_tags_for_session.md)
- [input_object_mut](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/input_object_mut.md)
- [sort_table_request_is_valid](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_validation/sort_table_request_is_valid.md)
- [invalid_table_sort_orders](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/state/invalid_table_sort_orders.md)
- [rop_error_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response.md)
- [response_handle_index](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index.md)
- [record_outlook_view_failure_trace_event](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_outlook_view_failure_trace_event.md)
- [sort_category_count](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/sort_category_count.md)
- [sort_expanded_count](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/sort_expanded_count.md)
- [format_contents_table_named_property_context](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/named_properties/format_contents_table_named_property_context.md)
- [log_outlook_contents_table_sort](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/table_queries/log_outlook_contents_table_sort.md)
- [sort_table_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/sort_table_response.md)
- [record_normal_inbox_table_lifecycle](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_lifecycle/record_normal_inbox_table_lifecycle.md)
- [input_handle_index](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/input_handle_index.md)

# Called by

- [append_table_control_dispatch_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_table_control_dispatch_response.md)