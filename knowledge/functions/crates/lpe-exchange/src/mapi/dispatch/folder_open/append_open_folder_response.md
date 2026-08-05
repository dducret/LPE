---
type: Rust Function
title: append_open_folder_response
resource: crates/lpe-exchange/src/mapi/dispatch/folder_open.rs#L38-L572
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/session/input_handle
  - functions/crates/lpe-exchange/src/mapi/session/input_object
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/mapi_object_debug_folder_id
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/format_handle_lineage_context
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/resolve_special_folder_alias
  - functions/crates/lpe-exchange/src/mapi/rop/folder_row_for_id
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/collaboration_folder_for_id
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/public_folder_for_id
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/search_folder_definition_was_deleted
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/search_folder_definition_for_folder_id
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/search_folder_definition
  - functions/crates/lpe-exchange/src/mapi/tables/folders/is_advertised_special_folder
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/open_folder/debug_open_folder_metadata
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/calendar/log_calendar_folder_contract
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/special_folders/log_special_folder_contract
  - functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/public_folder_replica_server_names
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_opened_folder
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/values/mapi_value_debug_string
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/values/mapi_value_debug_u32
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/values/mapi_value_debug_binary_decode
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/values/mapi_value_debug_bool
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/default_folders/default_view_entry_id_for_debug
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/default_folders/default_view_entry_id_target_for_debug
  - functions/crates/lpe-exchange/src/mapi/properties/views/default_view_supported_folder
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/debug_advertised_default_named_view
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_default_view_advertised
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/default_view_folder_open_match_state
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/default_view_advertisement_state_for_folder
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/allocate_output_handle_avoiding
  - functions/crates/lpe-exchange/src/mapi/session/set_handle_slot
  - functions/crates/lpe-exchange/src/mapi/rop/responses/rop_open_folder_response
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_default_view_folder_opened
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_first_inbox_loop_transition_context
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_inbox_open_folder_probe
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_last_inbox_open_folder_context
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_recent_probe_action
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/post_hierarchy/inbox_post_fai_reopen_stall_observed
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_post_hierarchy_request_contract
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/post_hierarchy/post_hierarchy_open_folder_contract
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/mark_post_inbox_fai_reopen_logged
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/mark_post_rule_organizer_stream_reopen_logged
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/post_hierarchy/format_inbox_open_loop_summary
  - functions/crates/lpe-exchange/src/mapi/record_mapi_outlook_view_repeated_inbox_open_after_common_views
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_outlook_view_failure_trace_event
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/mark_post_common_views_inbox_open_loop_metric_logged
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/mark_inbox_loop_transition_logged
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/log_outlook_bootstrap_phase
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/folder_open/append_folder_open_dispatch_response
---

# Signature

`pub(super) fn append_open_folder_response( principal: &AccountPrincipal, request_id: &str, session: &mut MapiSession, handle_slots: &mut Vec<u32>, request: &RopRequest, mailboxes: &[JmapMailbox], emails: &[JmapEmail], snapshot: &MapiMailStoreSnapshot, same_execute_released_handles: &HashSet<u32>, responses: &mut Vec<u8>, output_handles: &mut Vec<u32>, )`

# Calls

- [input_handle](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/input_handle.md)
- [input_object](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/input_object.md)
- [mapi_object_debug_folder_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/mapi_object_debug_folder_id.md)
- [format_handle_lineage_context](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/format_handle_lineage_context.md)
- [resolve_special_folder_alias](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/resolve_special_folder_alias.md)
- [folder_row_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/folder_row_for_id.md)
- [collaboration_folder_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/collaboration_folder_for_id.md)
- [public_folder_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/public_folder_for_id.md)
- [search_folder_definition_was_deleted](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/search_folder_definition_was_deleted.md)
- [search_folder_definition_for_folder_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/search_folder_definition_for_folder_id.md)
- [search_folder_definition](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/search_folder_definition.md)
- [is_advertised_special_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/is_advertised_special_folder.md)
- [debug_open_folder_metadata](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/open_folder/debug_open_folder_metadata.md)
- [log_calendar_folder_contract](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/calendar/log_calendar_folder_contract.md)
- [log_special_folder_contract](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/special_folders/log_special_folder_contract.md)
- [rop_error_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response.md)
- [public_folder_replica_server_names](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/public_folder_replica_server_names.md)
- [record_opened_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_opened_folder.md)
- [mapi_value_debug_string](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/values/mapi_value_debug_string.md)
- [mapi_value_debug_u32](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/values/mapi_value_debug_u32.md)
- [mapi_value_debug_binary_decode](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/values/mapi_value_debug_binary_decode.md)
- [mapi_value_debug_bool](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/values/mapi_value_debug_bool.md)
- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [default_view_entry_id_for_debug](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/default_folders/default_view_entry_id_for_debug.md)
- [default_view_entry_id_target_for_debug](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/default_folders/default_view_entry_id_target_for_debug.md)
- [default_view_supported_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/views/default_view_supported_folder.md)
- [debug_advertised_default_named_view](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/debug_advertised_default_named_view.md)
- [record_default_view_advertised](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_default_view_advertised.md)
- [default_view_folder_open_match_state](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/default_view_folder_open_match_state.md)
- [default_view_advertisement_state_for_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/default_view_advertisement_state_for_folder.md)
- [allocate_output_handle_avoiding](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/allocate_output_handle_avoiding.md)
- [set_handle_slot](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/set_handle_slot.md)
- [rop_open_folder_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_open_folder_response.md)
- [record_default_view_folder_opened](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_default_view_folder_opened.md)
- [record_first_inbox_loop_transition_context](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_first_inbox_loop_transition_context.md)
- [record_inbox_open_folder_probe](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_inbox_open_folder_probe.md)
- [record_last_inbox_open_folder_context](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_last_inbox_open_folder_context.md)
- [record_recent_probe_action](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_recent_probe_action.md)
- [inbox_post_fai_reopen_stall_observed](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/post_hierarchy/inbox_post_fai_reopen_stall_observed.md)
- [record_post_hierarchy_request_contract](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_post_hierarchy_request_contract.md)
- [post_hierarchy_open_folder_contract](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/post_hierarchy/post_hierarchy_open_folder_contract.md)
- [mark_post_inbox_fai_reopen_logged](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/mark_post_inbox_fai_reopen_logged.md)
- [mark_post_rule_organizer_stream_reopen_logged](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/mark_post_rule_organizer_stream_reopen_logged.md)
- [format_inbox_open_loop_summary](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/post_hierarchy/format_inbox_open_loop_summary.md)
- [record_mapi_outlook_view_repeated_inbox_open_after_common_views](../../../../../../../functions/crates/lpe-exchange/src/mapi/record_mapi_outlook_view_repeated_inbox_open_after_common_views.md)
- [record_outlook_view_failure_trace_event](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_outlook_view_failure_trace_event.md)
- [mark_post_common_views_inbox_open_loop_metric_logged](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/mark_post_common_views_inbox_open_loop_metric_logged.md)
- [mark_inbox_loop_transition_logged](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/mark_inbox_loop_transition_logged.md)
- [log_outlook_bootstrap_phase](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/log_outlook_bootstrap_phase.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [append_folder_open_dispatch_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folder_open/append_folder_open_dispatch_response.md)