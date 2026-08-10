---
type: Rust Function
title: input_object
resource: crates/lpe-exchange/src/mapi/session.rs#L1268-L1275
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/session/input_handle
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/attachments/append_get_valid_attachments_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/attachments/append_get_attachment_table_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/attachments/append_open_attachment_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/attachments/append_create_attachment_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/attachments/append_delete_attachment_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/attachments/attachment_overlay_object
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute/append_execute_status_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/folder_create/append_create_folder_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/folder_open/append_open_folder_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/folders/append_empty_folder_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/folders/append_delete_folder_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/folders/append_folder_move_copy_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/folders/hydrate_folder_handle_properties_for_request
  - functions/crates/lpe-exchange/src/mapi/dispatch/logon/private_logon_request_handle
  - functions/crates/lpe-exchange/src/mapi/dispatch/logon/append_address_types_dispatch_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/message_move_copy/append_move_copy_messages_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/messages/append_create_message_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/messages/append_reload_cached_information_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/messages/append_set_message_read_flag_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/messages/append_set_read_flags_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/messages/append_delete_messages_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/messages/append_message_status_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/named_properties/record_outlook_umolk_named_property_probe
  - functions/crates/lpe-exchange/src/mapi/dispatch/named_properties/record_post_calendar_query_position_named_property_probe
  - functions/crates/lpe-exchange/src/mapi/dispatch/notification_subscriptions/append_register_notification_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/permissions/append_get_permissions_table_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/permissions/append_modify_permissions_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_get_properties_all_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_get_properties_list_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_copy_to_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_copy_properties_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_commit_stream_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/property_mutations/append_set_properties_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/property_mutations/append_delete_properties_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/recipients/append_read_recipients_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/recipients/append_remove_all_recipients_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/recipients/append_modify_recipients_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/release/append_release_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/rules/append_get_rules_table_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/rules/append_modify_rules_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/append_set_search_criteria_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/append_get_search_criteria_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/submission/append_transport_folder_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/submission/append_options_data_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_configure/append_synchronization_configure_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_get_buffer/fast_transfer_source_transfer_position
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/staged_fast_transfer_destination_buffer
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/append_tell_version_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/append_fast_transfer_source_copy_messages_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/append_fast_transfer_source_copy_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/append_synchronization_get_transfer_state_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/append_synchronization_open_collector_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_deletes/append_synchronization_import_deletes_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_hierarchy/append_synchronization_import_hierarchy_change_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_message/append_synchronization_import_message_change_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_message_move/append_synchronization_import_message_move_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_read_state/append_synchronization_import_read_state_changes_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_table_control_dispatch_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_set_columns_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_sort_table_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_restrict_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_restrict_table_control_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_query_rows_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_find_row_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_open/append_open_table_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/append_table_control_response
---

# Signature

`pub(in crate::mapi) fn input_object<'a>( session: &'a MapiSession, input_handles: &[u32], request: &RopRequest, ) -> Option<&'a MapiObject>`

# Calls

- [input_handle](../../../../../../functions/crates/lpe-exchange/src/mapi/session/input_handle.md)
- [get](../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [append_get_valid_attachments_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/attachments/append_get_valid_attachments_response.md)
- [append_get_attachment_table_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/attachments/append_get_attachment_table_response.md)
- [append_open_attachment_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/attachments/append_open_attachment_response.md)
- [append_create_attachment_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/attachments/append_create_attachment_response.md)
- [append_delete_attachment_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/attachments/append_delete_attachment_response.md)
- [attachment_overlay_object](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/attachments/attachment_overlay_object.md)
- [append_execute_status_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute/append_execute_status_response.md)
- [append_create_folder_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folder_create/append_create_folder_response.md)
- [append_open_folder_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folder_open/append_open_folder_response.md)
- [append_empty_folder_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folders/append_empty_folder_response.md)
- [append_delete_folder_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folders/append_delete_folder_response.md)
- [append_folder_move_copy_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folders/append_folder_move_copy_response.md)
- [hydrate_folder_handle_properties_for_request](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folders/hydrate_folder_handle_properties_for_request.md)
- [private_logon_request_handle](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/logon/private_logon_request_handle.md)
- [append_address_types_dispatch_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/logon/append_address_types_dispatch_response.md)
- [append_move_copy_messages_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/message_move_copy/append_move_copy_messages_response.md)
- [append_create_message_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/messages/append_create_message_response.md)
- [append_reload_cached_information_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/messages/append_reload_cached_information_response.md)
- [append_set_message_read_flag_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/messages/append_set_message_read_flag_response.md)
- [append_set_read_flags_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/messages/append_set_read_flags_response.md)
- [append_delete_messages_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/messages/append_delete_messages_response.md)
- [append_message_status_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/messages/append_message_status_response.md)
- [record_outlook_umolk_named_property_probe](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/named_properties/record_outlook_umolk_named_property_probe.md)
- [record_post_calendar_query_position_named_property_probe](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/named_properties/record_post_calendar_query_position_named_property_probe.md)
- [append_register_notification_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/notification_subscriptions/append_register_notification_response.md)
- [append_get_permissions_table_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/permissions/append_get_permissions_table_response.md)
- [append_modify_permissions_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/permissions/append_modify_permissions_response.md)
- [append_get_properties_all_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_get_properties_all_response.md)
- [append_get_properties_list_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_get_properties_list_response.md)
- [append_copy_to_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_copy_to_response.md)
- [append_copy_properties_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_copy_properties_response.md)
- [append_commit_stream_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_commit_stream_response.md)
- [append_set_properties_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/property_mutations/append_set_properties_response.md)
- [append_delete_properties_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/property_mutations/append_delete_properties_response.md)
- [append_read_recipients_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/recipients/append_read_recipients_response.md)
- [append_remove_all_recipients_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/recipients/append_remove_all_recipients_response.md)
- [append_modify_recipients_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/recipients/append_modify_recipients_response.md)
- [append_release_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/release/append_release_response.md)
- [append_get_rules_table_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/rules/append_get_rules_table_response.md)
- [append_modify_rules_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/rules/append_modify_rules_response.md)
- [append_set_search_criteria_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/append_set_search_criteria_response.md)
- [append_get_search_criteria_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/append_get_search_criteria_response.md)
- [append_transport_folder_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/submission/append_transport_folder_response.md)
- [append_options_data_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/submission/append_options_data_response.md)
- [append_synchronization_configure_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_configure/append_synchronization_configure_response.md)
- [fast_transfer_source_transfer_position](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_get_buffer/fast_transfer_source_transfer_position.md)
- [staged_fast_transfer_destination_buffer](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/staged_fast_transfer_destination_buffer.md)
- [append_tell_version_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/append_tell_version_response.md)
- [append_fast_transfer_source_copy_messages_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/append_fast_transfer_source_copy_messages_response.md)
- [append_fast_transfer_source_copy_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/append_fast_transfer_source_copy_response.md)
- [append_synchronization_get_transfer_state_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/append_synchronization_get_transfer_state_response.md)
- [append_synchronization_open_collector_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/append_synchronization_open_collector_response.md)
- [append_synchronization_import_deletes_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_deletes/append_synchronization_import_deletes_response.md)
- [append_synchronization_import_hierarchy_change_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_hierarchy/append_synchronization_import_hierarchy_change_response.md)
- [append_synchronization_import_message_change_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_message/append_synchronization_import_message_change_response.md)
- [append_synchronization_import_message_move_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_message_move/append_synchronization_import_message_move_response.md)
- [append_synchronization_import_read_state_changes_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_read_state/append_synchronization_import_read_state_changes_response.md)
- [append_table_control_dispatch_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_table_control_dispatch_response.md)
- [append_set_columns_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_set_columns_response.md)
- [append_sort_table_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_sort_table_response.md)
- [append_restrict_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_restrict_response.md)
- [append_restrict_table_control_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_restrict_table_control_response.md)
- [append_query_rows_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_query_rows_response.md)
- [append_find_row_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_find_row_response.md)
- [append_open_table_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_open/append_open_table_response.md)
- [append_table_control_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/append_table_control_response.md)