---
type: Rust Function
title: append_open_table_response
resource: crates/lpe-exchange/src/mapi/dispatch/table_open.rs#L61-L444
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/session/input_handle
  - functions/crates/lpe-exchange/src/mapi/rop/errors/rop_handle_index_error_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_validation/hierarchy_table_flags_are_valid
  - functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response
  - functions/crates/lpe-exchange/src/mapi/session/input_object
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/mapi_object_debug_folder_id
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/format_handle_lineage_context
  - functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_depth_folder_ids_excluding_deleted
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/allocate_output_handle
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/hierarchy_table_object
  - functions/crates/lpe-exchange/src/mapi/session/table_notifications/MapiSession/remember_table_notification_eligibility
  - functions/crates/lpe-exchange/src/mapi/session/set_handle_slot
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/public_folders
  - functions/crates/lpe-exchange/src/mapi/tables/counts/hierarchy_row_count_excluding_deleted
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/get_hierarchy_table_response
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_last_table_context
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_outlook_view_failure_trace_event
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_last_inbox_hierarchy_table_context
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_recent_probe_action
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/log_outlook_bootstrap_phase
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_validation/contents_table_flags_error
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/public_folder_for_id
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/folder_access_for_principal
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/open_folder/debug_open_folder_metadata
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_open/default_view_contents_table_initial_sort
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_open/contents_table_object_with_default_view_sort
  - functions/crates/lpe-exchange/src/mapi/tables/counts/contents_table_open_row_count
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/table_queries/log_outlook_contents_table_open
  - functions/crates/lpe-exchange/src/mapi/properties/views/default_view_supported_folder
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/format_debug_sort_orders
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_inbox_associated_contents_table
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_inbox_normal_contents_table
  - functions/crates/lpe-exchange/src/mapi/record_mapi_outlook_view_inbox_normal_contents_opened
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_lifecycle/record_normal_inbox_table_lifecycle
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/input_handle_index
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_last_inbox_contents_table_context
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/get_contents_table_response
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_open/append_table_open_dispatch_response
---

# Signature

`pub(super) async fn append_open_table_response<S>( store: &S, principal: &AccountPrincipal, request_id: &str, request_rop_names: &str, session: &mut MapiSession, handle_slots: &mut Vec<u32>, request: &RopRequest, logon_id: u8, mailboxes: &[JmapMailbox], emails: &[JmapEmail], snapshot: &MapiMailStoreSnapshot, responses: &mut Vec<u8>, output_handles: &mut Vec<u32>, ) where S: ExchangeStore,`

# Calls

- [input_handle](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/input_handle.md)
- [rop_handle_index_error_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/errors/rop_handle_index_error_response.md)
- [hierarchy_table_flags_are_valid](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_validation/hierarchy_table_flags_are_valid.md)
- [rop_error_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response.md)
- [input_object](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/input_object.md)
- [mapi_object_debug_folder_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/mapi_object_debug_folder_id.md)
- [format_handle_lineage_context](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/format_handle_lineage_context.md)
- [hierarchy_depth_folder_ids_excluding_deleted](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_depth_folder_ids_excluding_deleted.md)
- [allocate_output_handle](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/allocate_output_handle.md)
- [hierarchy_table_object](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/hierarchy_table_object.md)
- [remember_table_notification_eligibility](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/table_notifications/MapiSession/remember_table_notification_eligibility.md)
- [set_handle_slot](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/set_handle_slot.md)
- [public_folders](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/public_folders.md)
- [hierarchy_row_count_excluding_deleted](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/counts/hierarchy_row_count_excluding_deleted.md)
- [get_hierarchy_table_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/get_hierarchy_table_response.md)
- [record_last_table_context](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_last_table_context.md)
- [record_outlook_view_failure_trace_event](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_outlook_view_failure_trace_event.md)
- [record_last_inbox_hierarchy_table_context](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_last_inbox_hierarchy_table_context.md)
- [record_recent_probe_action](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_recent_probe_action.md)
- [log_outlook_bootstrap_phase](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/log_outlook_bootstrap_phase.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [contents_table_flags_error](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_validation/contents_table_flags_error.md)
- [public_folder_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/public_folder_for_id.md)
- [folder_access_for_principal](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/folder_access_for_principal.md)
- [debug_open_folder_metadata](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/open_folder/debug_open_folder_metadata.md)
- [default_view_contents_table_initial_sort](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_open/default_view_contents_table_initial_sort.md)
- [contents_table_object_with_default_view_sort](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_open/contents_table_object_with_default_view_sort.md)
- [contents_table_open_row_count](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/counts/contents_table_open_row_count.md)
- [log_outlook_contents_table_open](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/table_queries/log_outlook_contents_table_open.md)
- [default_view_supported_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/views/default_view_supported_folder.md)
- [format_debug_sort_orders](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/format_debug_sort_orders.md)
- [record_inbox_associated_contents_table](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_inbox_associated_contents_table.md)
- [record_inbox_normal_contents_table](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_inbox_normal_contents_table.md)
- [record_mapi_outlook_view_inbox_normal_contents_opened](../../../../../../../functions/crates/lpe-exchange/src/mapi/record_mapi_outlook_view_inbox_normal_contents_opened.md)
- [record_normal_inbox_table_lifecycle](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_lifecycle/record_normal_inbox_table_lifecycle.md)
- [input_handle_index](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/input_handle_index.md)
- [record_last_inbox_contents_table_context](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_last_inbox_contents_table_context.md)
- [get_contents_table_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/get_contents_table_response.md)

# Called by

- [append_table_open_dispatch_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_open/append_table_open_dispatch_response.md)