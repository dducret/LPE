---
type: Rust Function
title: input_object_mut
resource: crates/lpe-exchange/src/mapi/session.rs#L1277-L1284
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/session/input_handle
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/conversation_actions/stage_virtual_conversation_action_property_values
  - functions/crates/lpe-exchange/src/mapi/dispatch/conversation_actions/stage_virtual_conversation_action_property_delete
  - functions/crates/lpe-exchange/src/mapi/dispatch/event_transactions/stage_event_property_values
  - functions/crates/lpe-exchange/src/mapi/dispatch/event_transactions/stage_pending_event_property_values
  - functions/crates/lpe-exchange/src/mapi/dispatch/event_transactions/stage_event_property_deletions
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute/append_execute_status_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/folders/hydrate_folder_handle_properties_for_request
  - functions/crates/lpe-exchange/src/mapi/dispatch/navigation_shortcut_save/stage_existing_navigation_shortcut_property_values
  - functions/crates/lpe-exchange/src/mapi/dispatch/navigation_shortcut_save/stage_existing_navigation_shortcut_property_deletions
  - functions/crates/lpe-exchange/src/mapi/dispatch/property_mutations/append_set_properties_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/property_mutations/stage_delegate_freebusy_property_values
  - functions/crates/lpe-exchange/src/mapi/dispatch/property_mutations/append_delete_properties_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/recipients/append_remove_all_recipients_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/recipients/append_modify_recipients_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_get_buffer/append_fast_transfer_source_get_buffer_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/commit_fast_transfer_destination_buffer
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_upload_state/append_upload_state_stream_begin_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_upload_state/append_upload_state_stream_continue_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_upload_state/append_upload_state_stream_end_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_set_columns_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_sort_table_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_restrict_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_query_rows_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_find_row_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/append_free_bookmark_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/append_table_control_response
---

# Signature

`pub(in crate::mapi) fn input_object_mut<'a>( session: &'a mut MapiSession, input_handles: &[u32], request: &RopRequest, ) -> Option<&'a mut MapiObject>`

# Calls

- [input_handle](../../../../../../functions/crates/lpe-exchange/src/mapi/session/input_handle.md)

# Called by

- [stage_virtual_conversation_action_property_values](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/conversation_actions/stage_virtual_conversation_action_property_values.md)
- [stage_virtual_conversation_action_property_delete](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/conversation_actions/stage_virtual_conversation_action_property_delete.md)
- [stage_event_property_values](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/event_transactions/stage_event_property_values.md)
- [stage_pending_event_property_values](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/event_transactions/stage_pending_event_property_values.md)
- [stage_event_property_deletions](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/event_transactions/stage_event_property_deletions.md)
- [append_execute_status_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute/append_execute_status_response.md)
- [hydrate_folder_handle_properties_for_request](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folders/hydrate_folder_handle_properties_for_request.md)
- [stage_existing_navigation_shortcut_property_values](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/navigation_shortcut_save/stage_existing_navigation_shortcut_property_values.md)
- [stage_existing_navigation_shortcut_property_deletions](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/navigation_shortcut_save/stage_existing_navigation_shortcut_property_deletions.md)
- [append_set_properties_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/property_mutations/append_set_properties_response.md)
- [stage_delegate_freebusy_property_values](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/property_mutations/stage_delegate_freebusy_property_values.md)
- [append_delete_properties_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/property_mutations/append_delete_properties_response.md)
- [append_remove_all_recipients_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/recipients/append_remove_all_recipients_response.md)
- [append_modify_recipients_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/recipients/append_modify_recipients_response.md)
- [append_fast_transfer_source_get_buffer_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_get_buffer/append_fast_transfer_source_get_buffer_response.md)
- [commit_fast_transfer_destination_buffer](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/commit_fast_transfer_destination_buffer.md)
- [append_upload_state_stream_begin_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_upload_state/append_upload_state_stream_begin_response.md)
- [append_upload_state_stream_continue_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_upload_state/append_upload_state_stream_continue_response.md)
- [append_upload_state_stream_end_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_upload_state/append_upload_state_stream_end_response.md)
- [append_set_columns_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_set_columns_response.md)
- [append_sort_table_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_sort_table_response.md)
- [append_restrict_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_restrict_response.md)
- [append_query_rows_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_query_rows_response.md)
- [append_find_row_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_find_row_response.md)
- [append_free_bookmark_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/append_free_bookmark_response.md)
- [append_table_control_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/append_table_control_response.md)