---
type: Rust Function
title: append_set_columns_response
resource: crates/lpe-exchange/src/mapi/dispatch/table_controls.rs#L218-L579
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/property_tags
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/normalize_table_property_tags_for_session
  - functions/crates/lpe-exchange/src/mapi/session/input_handle
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/named_properties/format_debug_named_property_context
  - functions/crates/lpe-exchange/src/mapi/session/input_object
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/format_outlook_view_descriptor_named_property_context
  - functions/crates/lpe-exchange/src/mapi/session/input_object_mut
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_validation/set_columns_request_is_valid
  - functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_last_inbox_hierarchy_table_context
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/set_columns_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/table_queries/log_outlook_contents_table_set_columns
  - functions/crates/lpe-exchange/src/mapi/tables/counts/folder_message_count
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/format_outlook_view_handoff_table_contract
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/format_inbox_view_descriptor_set_columns_behavior_contract
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_normal_message_query_row_summary
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_lifecycle/format_visible_inbox_first_row_projection_audit
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/is_outlook_default_view_setcolumns_diagnostic_target
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_validation/set_columns_request_is_valid_for_rule_table
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_inbox_normal_contents_table_setcolumns
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_outlook_view_failure_trace_event
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_lifecycle/record_normal_inbox_table_lifecycle
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/input_handle_index
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_table_control_dispatch_response
---

# Signature

`pub(super) fn append_set_columns_response( principal: &AccountPrincipal, session: &mut MapiSession, handle_slots: &[u32], request: &RopRequest, request_id: &str, request_rop_names: &str, mailboxes: &[lpe_storage::JmapMailbox], emails: &[lpe_storage::JmapEmail], snapshot: &MapiMailStoreSnapshot, responses: &mut Vec<u8>, ) -> TableControlFlow`

# Calls

- [property_tags](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/property_tags.md)
- [normalize_table_property_tags_for_session](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/normalize_table_property_tags_for_session.md)
- [input_handle](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/input_handle.md)
- [format_debug_named_property_context](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/named_properties/format_debug_named_property_context.md)
- [input_object](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/input_object.md)
- [format_outlook_view_descriptor_named_property_context](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/format_outlook_view_descriptor_named_property_context.md)
- [input_object_mut](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/input_object_mut.md)
- [set_columns_request_is_valid](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_validation/set_columns_request_is_valid.md)
- [rop_error_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response.md)
- [response_handle_index](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index.md)
- [record_last_inbox_hierarchy_table_context](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_last_inbox_hierarchy_table_context.md)
- [set_columns_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/set_columns_response.md)
- [log_outlook_contents_table_set_columns](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/table_queries/log_outlook_contents_table_set_columns.md)
- [folder_message_count](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/counts/folder_message_count.md)
- [format_outlook_view_handoff_table_contract](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/format_outlook_view_handoff_table_contract.md)
- [format_inbox_view_descriptor_set_columns_behavior_contract](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/format_inbox_view_descriptor_set_columns_behavior_contract.md)
- [format_normal_message_query_row_summary](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_normal_message_query_row_summary.md)
- [format_visible_inbox_first_row_projection_audit](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_lifecycle/format_visible_inbox_first_row_projection_audit.md)
- [is_outlook_default_view_setcolumns_diagnostic_target](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/is_outlook_default_view_setcolumns_diagnostic_target.md)
- [set_columns_request_is_valid_for_rule_table](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_validation/set_columns_request_is_valid_for_rule_table.md)
- [record_inbox_normal_contents_table_setcolumns](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_inbox_normal_contents_table_setcolumns.md)
- [record_outlook_view_failure_trace_event](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_outlook_view_failure_trace_event.md)
- [record_normal_inbox_table_lifecycle](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_lifecycle/record_normal_inbox_table_lifecycle.md)
- [input_handle_index](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/input_handle_index.md)

# Called by

- [append_table_control_dispatch_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_table_control_dispatch_response.md)