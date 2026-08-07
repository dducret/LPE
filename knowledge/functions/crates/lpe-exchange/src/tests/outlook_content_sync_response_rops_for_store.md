---
type: Rust Function
title: outlook_content_sync_response_rops_for_store
resource: crates/lpe-exchange/src/tests/mod.rs#L15570-L15583
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/outlook_content_sync_response_rops_for_store_with_rops
  - functions/crates/lpe-exchange/src/tests/outlook_content_sync_request_rops
  called_by:
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_move_to_deleted_items_rekeys_and_projects_canonical_event
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_download_uses_uploaded_ics_state_without_server_checkpoint
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_ics_transient_read_state_uses_message_changes_not_synthetic_read_sets
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_ics_deleted_items_client_state_controls_baseline_versus_delta_selection
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/contacts/mapi_over_http_contacts_sync_exports_associated_config_deletes
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/contacts/mapi_over_http_contact_content_sync_exports_deletes
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/contacts/mapi_over_http_outlook_contact_sync_orders_special_messages_by_last_modification
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_conversation_action_content_sync_exports_deletes
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_associated_config_content_sync_exports_deletes
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_associated_config_delete_does_not_allocate_identity
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_sync_checkpoint_resumes_incremental_content_with_tombstone
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_content_sync_uses_retired_move_mid_for_source_tombstone
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_microsoft_oxcfxics_4_5_content_sync_stream_shape
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_content_sync_move_across_folders_exports_source_tombstone_and_target_change
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_content_sync_hard_delete_exports_tombstone_and_empty_final_state
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_content_sync_read_flag_update_exports_message_change_without_read_state
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_content_sync_incremental_does_not_leak_protected_bcc
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_content_sync_after_empty_folder_advances_empty_final_state
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_message_list_settings_import_preserves_outlook_identity_and_content
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_inbox_fai_download_honors_uploaded_state_with_empty_normal_cnset
---

# Signature

`async fn outlook_content_sync_response_rops_for_store<S>( store: S, folder_id: u64, state_properties: &[(u32, Vec<u8>)], ) -> Vec<u8> where S: ExchangeStore + Clone + Send + Sync + 'static,`

# Calls

- [outlook_content_sync_response_rops_for_store_with_rops](../../../../../functions/crates/lpe-exchange/src/tests/outlook_content_sync_response_rops_for_store_with_rops.md)
- [outlook_content_sync_request_rops](../../../../../functions/crates/lpe-exchange/src/tests/outlook_content_sync_request_rops.md)

# Called by

- [mapi_over_http_calendar_move_to_deleted_items_rekeys_and_projects_canonical_event](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_move_to_deleted_items_rekeys_and_projects_canonical_event.md)
- [mapi_over_http_calendar_download_uses_uploaded_ics_state_without_server_checkpoint](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_download_uses_uploaded_ics_state_without_server_checkpoint.md)
- [mapi_over_http_ics_transient_read_state_uses_message_changes_not_synthetic_read_sets](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_ics_transient_read_state_uses_message_changes_not_synthetic_read_sets.md)
- [mapi_over_http_ics_deleted_items_client_state_controls_baseline_versus_delta_selection](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_ics_deleted_items_client_state_controls_baseline_versus_delta_selection.md)
- [mapi_over_http_contacts_sync_exports_associated_config_deletes](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/contacts/mapi_over_http_contacts_sync_exports_associated_config_deletes.md)
- [mapi_over_http_contact_content_sync_exports_deletes](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/contacts/mapi_over_http_contact_content_sync_exports_deletes.md)
- [mapi_over_http_outlook_contact_sync_orders_special_messages_by_last_modification](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/contacts/mapi_over_http_outlook_contact_sync_orders_special_messages_by_last_modification.md)
- [mapi_over_http_conversation_action_content_sync_exports_deletes](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_conversation_action_content_sync_exports_deletes.md)
- [mapi_over_http_associated_config_content_sync_exports_deletes](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_associated_config_content_sync_exports_deletes.md)
- [mapi_over_http_associated_config_delete_does_not_allocate_identity](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_associated_config_delete_does_not_allocate_identity.md)
- [mapi_over_http_sync_checkpoint_resumes_incremental_content_with_tombstone](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_sync_checkpoint_resumes_incremental_content_with_tombstone.md)
- [mapi_over_http_content_sync_uses_retired_move_mid_for_source_tombstone](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_content_sync_uses_retired_move_mid_for_source_tombstone.md)
- [mapi_over_http_microsoft_oxcfxics_4_5_content_sync_stream_shape](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_microsoft_oxcfxics_4_5_content_sync_stream_shape.md)
- [mapi_over_http_content_sync_move_across_folders_exports_source_tombstone_and_target_change](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_content_sync_move_across_folders_exports_source_tombstone_and_target_change.md)
- [mapi_over_http_content_sync_hard_delete_exports_tombstone_and_empty_final_state](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_content_sync_hard_delete_exports_tombstone_and_empty_final_state.md)
- [mapi_over_http_content_sync_read_flag_update_exports_message_change_without_read_state](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_content_sync_read_flag_update_exports_message_change_without_read_state.md)
- [mapi_over_http_content_sync_incremental_does_not_leak_protected_bcc](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_content_sync_incremental_does_not_leak_protected_bcc.md)
- [mapi_over_http_content_sync_after_empty_folder_advances_empty_final_state](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_content_sync_after_empty_folder_advances_empty_final_state.md)
- [mapi_over_http_message_list_settings_import_preserves_outlook_identity_and_content](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_message_list_settings_import_preserves_outlook_identity_and_content.md)
- [mapi_over_http_inbox_fai_download_honors_uploaded_state_with_empty_normal_cnset](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_inbox_fai_download_honors_uploaded_state_with_empty_normal_cnset.md)