---
type: Rust Function
title: append_find_row_response
resource: crates/lpe-exchange/src/mapi/dispatch/table_controls.rs#L1387-L1519
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/session/input_object
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/format_optional_debug_handle
  - functions/crates/lpe-exchange/src/mapi/session/input_handle
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/format_debug_property_tags
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/format_debug_restriction
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/request_restriction_bytes
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/named_properties/format_contents_table_named_property_context
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/find_row_response
  - functions/crates/lpe-exchange/src/mapi/session/input_object_mut
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/table_queries/log_outlook_contents_table_find_row
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/format_inbox_associated_find_context
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_last_inbox_associated_find_context
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/inbox_associated_broad_findrow_matched
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_inbox_associated_broad_findrow
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/inbox_associated_exact_configuration_findrow_matched
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_inbox_associated_exact_findrow
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_inbox_associated_findrow_returned_content
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/read_response_error_code
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_outlook_view_failure_trace_event
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_lifecycle/record_normal_inbox_table_lifecycle
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/input_handle_index
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_inbox_normal_contents_table_find_row
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_table_control_dispatch_response
---

# Signature

`pub(super) fn append_find_row_response( principal: &AccountPrincipal, session: &mut MapiSession, handle_slots: &[u32], request: &RopRequest, request_id: &str, request_rop_names: &str, mailboxes: &[lpe_storage::JmapMailbox], emails: &[lpe_storage::JmapEmail], snapshot: &MapiMailStoreSnapshot, responses: &mut Vec<u8>, )`

# Calls

- [input_object](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/input_object.md)
- [format_optional_debug_handle](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/format_optional_debug_handle.md)
- [input_handle](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/input_handle.md)
- [format_debug_property_tags](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/format_debug_property_tags.md)
- [format_debug_restriction](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/format_debug_restriction.md)
- [request_restriction_bytes](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/request_restriction_bytes.md)
- [format_contents_table_named_property_context](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/named_properties/format_contents_table_named_property_context.md)
- [find_row_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/find_row_response.md)
- [input_object_mut](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/input_object_mut.md)
- [log_outlook_contents_table_find_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/table_queries/log_outlook_contents_table_find_row.md)
- [format_inbox_associated_find_context](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/format_inbox_associated_find_context.md)
- [record_last_inbox_associated_find_context](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_last_inbox_associated_find_context.md)
- [inbox_associated_broad_findrow_matched](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/inbox_associated_broad_findrow_matched.md)
- [record_inbox_associated_broad_findrow](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_inbox_associated_broad_findrow.md)
- [inbox_associated_exact_configuration_findrow_matched](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/inbox_associated_exact_configuration_findrow_matched.md)
- [record_inbox_associated_exact_findrow](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_inbox_associated_exact_findrow.md)
- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [record_inbox_associated_findrow_returned_content](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_inbox_associated_findrow_returned_content.md)
- [read_response_error_code](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/read_response_error_code.md)
- [record_outlook_view_failure_trace_event](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_outlook_view_failure_trace_event.md)
- [record_normal_inbox_table_lifecycle](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_lifecycle/record_normal_inbox_table_lifecycle.md)
- [input_handle_index](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/input_handle_index.md)
- [record_inbox_normal_contents_table_find_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_inbox_normal_contents_table_find_row.md)

# Called by

- [append_table_control_dispatch_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_table_control_dispatch_response.md)