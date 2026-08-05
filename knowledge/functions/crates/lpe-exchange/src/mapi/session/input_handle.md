---
type: Rust Function
title: input_handle
resource: crates/lpe-exchange/src/mapi/session.rs#L1354-L1359
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/input_handle_index
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute_rops
  - functions/crates/lpe-exchange/src/mapi/dispatch/attachments/append_get_valid_attachments_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/attachments/append_get_attachment_table_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/attachments/append_open_attachment_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/attachments/append_create_attachment_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/attachments/append_delete_attachment_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/attachments/append_open_embedded_message_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/attachments/append_save_changes_attachment_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/attachments/attachment_overlay_object
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute/rop_buffer_is_store_independent_special_folder_getprops_probe
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute/append_execute_status_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/folder_open/append_open_folder_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/folders/hydrate_folder_handle_properties_for_request
  - functions/crates/lpe-exchange/src/mapi/dispatch/local_replica_sync/append_set_local_replica_midset_deleted_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/local_replica_sync/append_get_local_replica_ids_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/logon/private_logon_request_handle
  - functions/crates/lpe-exchange/src/mapi/dispatch/logon/append_store_state_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/message_save/append_save_changes_message_route_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/messages/stage_message_property_values
  - functions/crates/lpe-exchange/src/mapi/dispatch/notification_subscriptions/append_register_notification_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_get_properties_specific_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_get_stream_size_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_open_stream_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_read_stream_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_clone_stream_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_stream_region_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_seek_stream_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_set_stream_size_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_write_stream_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_copy_to_stream_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_copy_properties_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_commit_stream_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/property_mutations/clear_folder_profile_property_tombstones
  - functions/crates/lpe-exchange/src/mapi/dispatch/property_mutations/mark_folder_profile_property_tombstones
  - functions/crates/lpe-exchange/src/mapi/dispatch/recipients/append_read_recipients_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/recipients/append_remove_all_recipients_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/recipients/append_modify_recipients_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/release/append_release_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/submission/append_spooler_advisory_dispatch_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/submission/append_deferred_action_messages_dispatch_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/submission/append_submit_message_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/append_fast_transfer_destination_configure_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_set_columns_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_sort_table_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_restrict_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_query_rows_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_find_row_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_lifecycle/normal_inbox_table_lifecycle_details
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_open/append_open_table_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/apply_outlook_smart_input_variant_before_query_rows
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/append_table_control_response
  - functions/crates/lpe-exchange/src/mapi/session/input_object
  - functions/crates/lpe-exchange/src/mapi/session/input_object_mut
  - functions/crates/lpe-exchange/src/mapi/session/table_notifications/MapiSession/activate_table_notifications_for_request
  - functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/extend_access_plan_for_request
  - functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/rop_uses_session_state_only
  - functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/simulate_table_access
---

# Signature

`pub(in crate::mapi) fn input_handle(input_handles: &[u32], request: &RopRequest) -> Option<u32>`

# Calls

- [get](../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [input_handle_index](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/input_handle_index.md)

# Called by

- [execute_rops](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute_rops.md)
- [append_get_valid_attachments_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/attachments/append_get_valid_attachments_response.md)
- [append_get_attachment_table_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/attachments/append_get_attachment_table_response.md)
- [append_open_attachment_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/attachments/append_open_attachment_response.md)
- [append_create_attachment_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/attachments/append_create_attachment_response.md)
- [append_delete_attachment_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/attachments/append_delete_attachment_response.md)
- [append_open_embedded_message_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/attachments/append_open_embedded_message_response.md)
- [append_save_changes_attachment_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/attachments/append_save_changes_attachment_response.md)
- [attachment_overlay_object](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/attachments/attachment_overlay_object.md)
- [rop_buffer_is_store_independent_special_folder_getprops_probe](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute/rop_buffer_is_store_independent_special_folder_getprops_probe.md)
- [append_execute_status_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute/append_execute_status_response.md)
- [append_open_folder_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folder_open/append_open_folder_response.md)
- [hydrate_folder_handle_properties_for_request](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folders/hydrate_folder_handle_properties_for_request.md)
- [append_set_local_replica_midset_deleted_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/local_replica_sync/append_set_local_replica_midset_deleted_response.md)
- [append_get_local_replica_ids_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/local_replica_sync/append_get_local_replica_ids_response.md)
- [private_logon_request_handle](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/logon/private_logon_request_handle.md)
- [append_store_state_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/logon/append_store_state_response.md)
- [append_save_changes_message_route_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/message_save/append_save_changes_message_route_response.md)
- [stage_message_property_values](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/messages/stage_message_property_values.md)
- [append_register_notification_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/notification_subscriptions/append_register_notification_response.md)
- [append_get_properties_specific_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_get_properties_specific_response.md)
- [append_get_stream_size_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_get_stream_size_response.md)
- [append_open_stream_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_open_stream_response.md)
- [append_read_stream_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_read_stream_response.md)
- [append_clone_stream_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_clone_stream_response.md)
- [append_stream_region_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_stream_region_response.md)
- [append_seek_stream_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_seek_stream_response.md)
- [append_set_stream_size_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_set_stream_size_response.md)
- [append_write_stream_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_write_stream_response.md)
- [append_copy_to_stream_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_copy_to_stream_response.md)
- [append_copy_properties_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_copy_properties_response.md)
- [append_commit_stream_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_commit_stream_response.md)
- [clear_folder_profile_property_tombstones](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/property_mutations/clear_folder_profile_property_tombstones.md)
- [mark_folder_profile_property_tombstones](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/property_mutations/mark_folder_profile_property_tombstones.md)
- [append_read_recipients_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/recipients/append_read_recipients_response.md)
- [append_remove_all_recipients_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/recipients/append_remove_all_recipients_response.md)
- [append_modify_recipients_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/recipients/append_modify_recipients_response.md)
- [append_release_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/release/append_release_response.md)
- [append_spooler_advisory_dispatch_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/submission/append_spooler_advisory_dispatch_response.md)
- [append_deferred_action_messages_dispatch_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/submission/append_deferred_action_messages_dispatch_response.md)
- [append_submit_message_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/submission/append_submit_message_response.md)
- [append_fast_transfer_destination_configure_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/append_fast_transfer_destination_configure_response.md)
- [append_set_columns_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_set_columns_response.md)
- [append_sort_table_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_sort_table_response.md)
- [append_restrict_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_restrict_response.md)
- [append_query_rows_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_query_rows_response.md)
- [append_find_row_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_find_row_response.md)
- [normal_inbox_table_lifecycle_details](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_lifecycle/normal_inbox_table_lifecycle_details.md)
- [append_open_table_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_open/append_open_table_response.md)
- [apply_outlook_smart_input_variant_before_query_rows](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/apply_outlook_smart_input_variant_before_query_rows.md)
- [append_table_control_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/append_table_control_response.md)
- [input_object](../../../../../../functions/crates/lpe-exchange/src/mapi/session/input_object.md)
- [input_object_mut](../../../../../../functions/crates/lpe-exchange/src/mapi/session/input_object_mut.md)
- [activate_table_notifications_for_request](../../../../../../functions/crates/lpe-exchange/src/mapi/session/table_notifications/MapiSession/activate_table_notifications_for_request.md)
- [extend_access_plan_for_request](../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/extend_access_plan_for_request.md)
- [rop_uses_session_state_only](../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/rop_uses_session_state_only.md)
- [simulate_table_access](../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/simulate_table_access.md)