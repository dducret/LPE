---
type: Rust Function
title: execute_rops_response_rops
resource: crates/lpe-exchange/src/tests/mod.rs#L15472-L15496
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/mapi_headers
  - functions/crates/lpe-exchange/src/tests/mapi_cookie_header
  - functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str
  - functions/crates/lpe-exchange/src/tests/execute_body
  - functions/crates/lpe-exchange/src/tests/response_rops_from_execute_response
  called_by:
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_get_properties_all_lists_entry_id_identity
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_get_properties_list_advertises_entry_id_identity
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_get_receive_folder_maps_appointments_to_calendar
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_store_get_properties_all_lists_calendar_default_entry_id
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_store_get_properties_list_advertises_calendar_default_entry_id
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_store_get_properties_specific_returns_calendar_default_entry_id
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_root_get_properties_all_lists_calendar_default_entry_id
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_root_get_properties_list_advertises_calendar_default_entry_id
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_root_get_properties_specific_returns_calendar_default_entry_id
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_root_get_properties_specific_returns_collaboration_default_entry_ids
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_inbox_get_properties_all_lists_calendar_default_entry_id
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_inbox_get_properties_list_advertises_calendar_default_entry_id
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_inbox_get_properties_specific_returns_calendar_default_entry_id
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_fast_transfer_destination_put_buffer_extended_is_parseable
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_fast_transfer_destination_rejects_wrong_target_handle
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_unknown_and_reserved_rops_terminate_current_buffer
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_microsoft_transport_info_rops_reject_missing_input_handle_without_batch_drift
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_malformed_rop_terminates_current_buffer
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/contacts/mapi_over_http_delete_contact_virtual_folder_is_noop_acknowledged
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/hierarchy/mapi_over_http_open_folder_rejects_unlearned_client_local_folder_id
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/logon_profile/mapi_over_http_get_receive_folder_requires_private_logon_handle
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/logon_profile/mapi_over_http_set_receive_folder_requires_private_logon_handle
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/logon_profile/mapi_over_http_microsoft_get_store_state_accepts_live_handle_without_batch_drift
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/logon_profile/mapi_over_http_get_receive_folder_preserves_ipm_note_inbox_mapping
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_microsoft_create_message_initializes_documented_properties
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_pending_message_display_recipients_follow_modify_recipients
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_fast_transfer_destination_rejects_marker_and_subobject_streams
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_fast_transfer_destination_rejects_unsupported_property_type
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_microsoft_copy_to_null_destination_response_keeps_batch_aligned
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_microsoft_copy_properties_null_destination_response_keeps_batch_aligned
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_microsoft_copy_properties_empty_tag_list_succeeds_as_noop
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_microsoft_property_rops_reject_missing_input_handle_without_batch_drift
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_unknown_property_type_terminates_current_buffer
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/public_folders/mapi_over_http_microsoft_public_folder_replica_rops_require_logon_handle_and_shape
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/public_folders/mapi_over_http_public_folder_logon_is_supported_without_private_store_flag
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/public_folders/mapi_over_http_public_folder_logon_exposes_empty_public_hierarchy_table
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_unknown_sync_type_terminates_current_buffer
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_unknown_fasttransfer_marker_terminates_current_buffer
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/tables/mapi_over_http_get_receive_folder_table_requires_private_logon_handle
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/tables/mapi_over_http_known_unmodeled_table_column_type_does_not_abort_buffer
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/tables/mapi_over_http_unknown_restriction_type_terminates_current_buffer
---

# Signature

`async fn execute_rops_response_rops(rops: &[u8], handles: &[u32]) -> Vec<u8>`

# Calls

- [mapi_headers](../../../../../functions/crates/lpe-exchange/src/tests/mapi_headers.md)
- [mapi_cookie_header](../../../../../functions/crates/lpe-exchange/src/tests/mapi_cookie_header.md)
- [from_str](../../../../../functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str.md)
- [execute_body](../../../../../functions/crates/lpe-exchange/src/tests/execute_body.md)
- [response_rops_from_execute_response](../../../../../functions/crates/lpe-exchange/src/tests/response_rops_from_execute_response.md)

# Called by

- [mapi_over_http_calendar_get_properties_all_lists_entry_id_identity](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_get_properties_all_lists_entry_id_identity.md)
- [mapi_over_http_calendar_get_properties_list_advertises_entry_id_identity](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_get_properties_list_advertises_entry_id_identity.md)
- [mapi_over_http_get_receive_folder_maps_appointments_to_calendar](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_get_receive_folder_maps_appointments_to_calendar.md)
- [mapi_over_http_store_get_properties_all_lists_calendar_default_entry_id](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_store_get_properties_all_lists_calendar_default_entry_id.md)
- [mapi_over_http_store_get_properties_list_advertises_calendar_default_entry_id](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_store_get_properties_list_advertises_calendar_default_entry_id.md)
- [mapi_over_http_store_get_properties_specific_returns_calendar_default_entry_id](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_store_get_properties_specific_returns_calendar_default_entry_id.md)
- [mapi_over_http_root_get_properties_all_lists_calendar_default_entry_id](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_root_get_properties_all_lists_calendar_default_entry_id.md)
- [mapi_over_http_root_get_properties_list_advertises_calendar_default_entry_id](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_root_get_properties_list_advertises_calendar_default_entry_id.md)
- [mapi_over_http_root_get_properties_specific_returns_calendar_default_entry_id](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_root_get_properties_specific_returns_calendar_default_entry_id.md)
- [mapi_over_http_root_get_properties_specific_returns_collaboration_default_entry_ids](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_root_get_properties_specific_returns_collaboration_default_entry_ids.md)
- [mapi_over_http_inbox_get_properties_all_lists_calendar_default_entry_id](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_inbox_get_properties_all_lists_calendar_default_entry_id.md)
- [mapi_over_http_inbox_get_properties_list_advertises_calendar_default_entry_id](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_inbox_get_properties_list_advertises_calendar_default_entry_id.md)
- [mapi_over_http_inbox_get_properties_specific_returns_calendar_default_entry_id](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_inbox_get_properties_specific_returns_calendar_default_entry_id.md)
- [mapi_over_http_fast_transfer_destination_put_buffer_extended_is_parseable](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_fast_transfer_destination_put_buffer_extended_is_parseable.md)
- [mapi_over_http_fast_transfer_destination_rejects_wrong_target_handle](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_fast_transfer_destination_rejects_wrong_target_handle.md)
- [mapi_over_http_unknown_and_reserved_rops_terminate_current_buffer](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_unknown_and_reserved_rops_terminate_current_buffer.md)
- [mapi_over_http_microsoft_transport_info_rops_reject_missing_input_handle_without_batch_drift](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_microsoft_transport_info_rops_reject_missing_input_handle_without_batch_drift.md)
- [mapi_over_http_malformed_rop_terminates_current_buffer](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_malformed_rop_terminates_current_buffer.md)
- [mapi_over_http_delete_contact_virtual_folder_is_noop_acknowledged](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/contacts/mapi_over_http_delete_contact_virtual_folder_is_noop_acknowledged.md)
- [mapi_over_http_open_folder_rejects_unlearned_client_local_folder_id](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/hierarchy/mapi_over_http_open_folder_rejects_unlearned_client_local_folder_id.md)
- [mapi_over_http_get_receive_folder_requires_private_logon_handle](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/logon_profile/mapi_over_http_get_receive_folder_requires_private_logon_handle.md)
- [mapi_over_http_set_receive_folder_requires_private_logon_handle](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/logon_profile/mapi_over_http_set_receive_folder_requires_private_logon_handle.md)
- [mapi_over_http_microsoft_get_store_state_accepts_live_handle_without_batch_drift](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/logon_profile/mapi_over_http_microsoft_get_store_state_accepts_live_handle_without_batch_drift.md)
- [mapi_over_http_get_receive_folder_preserves_ipm_note_inbox_mapping](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/logon_profile/mapi_over_http_get_receive_folder_preserves_ipm_note_inbox_mapping.md)
- [mapi_over_http_microsoft_create_message_initializes_documented_properties](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_microsoft_create_message_initializes_documented_properties.md)
- [mapi_over_http_pending_message_display_recipients_follow_modify_recipients](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_pending_message_display_recipients_follow_modify_recipients.md)
- [mapi_over_http_fast_transfer_destination_rejects_marker_and_subobject_streams](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_fast_transfer_destination_rejects_marker_and_subobject_streams.md)
- [mapi_over_http_fast_transfer_destination_rejects_unsupported_property_type](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_fast_transfer_destination_rejects_unsupported_property_type.md)
- [mapi_over_http_microsoft_copy_to_null_destination_response_keeps_batch_aligned](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_microsoft_copy_to_null_destination_response_keeps_batch_aligned.md)
- [mapi_over_http_microsoft_copy_properties_null_destination_response_keeps_batch_aligned](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_microsoft_copy_properties_null_destination_response_keeps_batch_aligned.md)
- [mapi_over_http_microsoft_copy_properties_empty_tag_list_succeeds_as_noop](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_microsoft_copy_properties_empty_tag_list_succeeds_as_noop.md)
- [mapi_over_http_microsoft_property_rops_reject_missing_input_handle_without_batch_drift](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_microsoft_property_rops_reject_missing_input_handle_without_batch_drift.md)
- [mapi_over_http_unknown_property_type_terminates_current_buffer](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_unknown_property_type_terminates_current_buffer.md)
- [mapi_over_http_microsoft_public_folder_replica_rops_require_logon_handle_and_shape](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/public_folders/mapi_over_http_microsoft_public_folder_replica_rops_require_logon_handle_and_shape.md)
- [mapi_over_http_public_folder_logon_is_supported_without_private_store_flag](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/public_folders/mapi_over_http_public_folder_logon_is_supported_without_private_store_flag.md)
- [mapi_over_http_public_folder_logon_exposes_empty_public_hierarchy_table](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/public_folders/mapi_over_http_public_folder_logon_exposes_empty_public_hierarchy_table.md)
- [mapi_over_http_unknown_sync_type_terminates_current_buffer](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_unknown_sync_type_terminates_current_buffer.md)
- [mapi_over_http_unknown_fasttransfer_marker_terminates_current_buffer](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_unknown_fasttransfer_marker_terminates_current_buffer.md)
- [mapi_over_http_get_receive_folder_table_requires_private_logon_handle](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/tables/mapi_over_http_get_receive_folder_table_requires_private_logon_handle.md)
- [mapi_over_http_known_unmodeled_table_column_type_does_not_abort_buffer](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/tables/mapi_over_http_known_unmodeled_table_column_type_does_not_abort_buffer.md)
- [mapi_over_http_unknown_restriction_type_terminates_current_buffer](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/tables/mapi_over_http_unknown_restriction_type_terminates_current_buffer.md)