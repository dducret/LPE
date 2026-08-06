---
type: Rust Function
title: mapi_fast_transfer_chunks
resource: crates/lpe-exchange/src/tests/mod.rs#L14824-L14855
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_fast_transfer_get_buffer_resumes_across_execute_requests
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/contacts/mapi_over_http_contact_link_copy_to_uses_message_content_root
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_associated_config_ignores_client_read_only_properties_in_postgresql
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_outlook_common_views_ics_import_stages_wlinks_until_save
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_ics_final_and_transfer_state_use_replguid_state_encoding
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_microsoft_oxcfxics_4_5_content_sync_stream_shape
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_fast_transfer_copy_to_message_excludes_requested_body_property
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_fast_transfer_copy_properties_message_includes_only_requested_subject
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_sync_source_transfer_state_returns_client_derived_final_state
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_upload_import_collector_handles_never_advance_download_checkpoints
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_microsoft_oxcfxics_4_2_1_message_upload_returns_transfer_state
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_microsoft_oxcfxics_4_2_2_message_delete_returns_transfer_state
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_message_list_settings_import_preserves_outlook_identity_and_content
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_inbox_fai_download_honors_uploaded_state_with_empty_normal_cnset
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_fast_transfer_copy_to_associated_config_message_succeeds
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_sync_import_soft_delete_moves_to_trash
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_sync_import_hierarchy_change_creates_canonical_mailbox
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_microsoft_oxcfxics_4_1_1_hierarchy_upload_returns_transfer_state
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_microsoft_oxcfxics_4_1_2_hierarchy_delete_returns_transfer_state
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_sync_import_hierarchy_change_accepts_existing_deleted_items
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_sync_import_hierarchy_change_keeps_hidden_system_folder_alias_in_cnset
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_sync_imported_junk_email_alias_is_reconciled_without_cnset_and_deleted_when_canonical_is_emitted
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_inbox_message_list_settings_import_preserves_outlook_system_properties_after_postgresql_reconnect
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/wlink_properties/mapi_over_http_wlink_client_properties_round_trip_postgresql_table_and_ics_after_reload
  - functions/crates/lpe-exchange/src/tests/assert_content_upload_final_state_includes
  - functions/crates/lpe-exchange/src/tests/strict_hierarchy_sync_transfer_from_response
  - functions/crates/lpe-exchange/src/tests/strict_content_sync_transfer_from_response
---

# Signature

`fn mapi_fast_transfer_chunks(bytes: &[u8]) -> Vec<(u16, Vec<u8>)>`

# Calls

- [get](../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [mapi_over_http_fast_transfer_get_buffer_resumes_across_execute_requests](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_fast_transfer_get_buffer_resumes_across_execute_requests.md)
- [mapi_over_http_contact_link_copy_to_uses_message_content_root](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/contacts/mapi_over_http_contact_link_copy_to_uses_message_content_root.md)
- [mapi_over_http_associated_config_ignores_client_read_only_properties_in_postgresql](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_associated_config_ignores_client_read_only_properties_in_postgresql.md)
- [mapi_over_http_outlook_common_views_ics_import_stages_wlinks_until_save](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_outlook_common_views_ics_import_stages_wlinks_until_save.md)
- [mapi_over_http_ics_final_and_transfer_state_use_replguid_state_encoding](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_ics_final_and_transfer_state_use_replguid_state_encoding.md)
- [mapi_over_http_microsoft_oxcfxics_4_5_content_sync_stream_shape](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_microsoft_oxcfxics_4_5_content_sync_stream_shape.md)
- [mapi_over_http_fast_transfer_copy_to_message_excludes_requested_body_property](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_fast_transfer_copy_to_message_excludes_requested_body_property.md)
- [mapi_over_http_fast_transfer_copy_properties_message_includes_only_requested_subject](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_fast_transfer_copy_properties_message_includes_only_requested_subject.md)
- [mapi_over_http_sync_source_transfer_state_returns_client_derived_final_state](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_sync_source_transfer_state_returns_client_derived_final_state.md)
- [mapi_over_http_upload_import_collector_handles_never_advance_download_checkpoints](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_upload_import_collector_handles_never_advance_download_checkpoints.md)
- [mapi_over_http_microsoft_oxcfxics_4_2_1_message_upload_returns_transfer_state](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_microsoft_oxcfxics_4_2_1_message_upload_returns_transfer_state.md)
- [mapi_over_http_microsoft_oxcfxics_4_2_2_message_delete_returns_transfer_state](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_microsoft_oxcfxics_4_2_2_message_delete_returns_transfer_state.md)
- [mapi_over_http_message_list_settings_import_preserves_outlook_identity_and_content](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_message_list_settings_import_preserves_outlook_identity_and_content.md)
- [mapi_over_http_inbox_fai_download_honors_uploaded_state_with_empty_normal_cnset](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_inbox_fai_download_honors_uploaded_state_with_empty_normal_cnset.md)
- [mapi_over_http_fast_transfer_copy_to_associated_config_message_succeeds](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_fast_transfer_copy_to_associated_config_message_succeeds.md)
- [mapi_over_http_sync_import_soft_delete_moves_to_trash](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_sync_import_soft_delete_moves_to_trash.md)
- [mapi_over_http_sync_import_hierarchy_change_creates_canonical_mailbox](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_sync_import_hierarchy_change_creates_canonical_mailbox.md)
- [mapi_over_http_microsoft_oxcfxics_4_1_1_hierarchy_upload_returns_transfer_state](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_microsoft_oxcfxics_4_1_1_hierarchy_upload_returns_transfer_state.md)
- [mapi_over_http_microsoft_oxcfxics_4_1_2_hierarchy_delete_returns_transfer_state](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_microsoft_oxcfxics_4_1_2_hierarchy_delete_returns_transfer_state.md)
- [mapi_over_http_sync_import_hierarchy_change_accepts_existing_deleted_items](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_sync_import_hierarchy_change_accepts_existing_deleted_items.md)
- [mapi_over_http_sync_import_hierarchy_change_keeps_hidden_system_folder_alias_in_cnset](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_sync_import_hierarchy_change_keeps_hidden_system_folder_alias_in_cnset.md)
- [mapi_over_http_sync_imported_junk_email_alias_is_reconciled_without_cnset_and_deleted_when_canonical_is_emitted](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_sync_imported_junk_email_alias_is_reconciled_without_cnset_and_deleted_when_canonical_is_emitted.md)
- [mapi_over_http_inbox_message_list_settings_import_preserves_outlook_system_properties_after_postgresql_reconnect](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_inbox_message_list_settings_import_preserves_outlook_system_properties_after_postgresql_reconnect.md)
- [mapi_over_http_wlink_client_properties_round_trip_postgresql_table_and_ics_after_reload](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/wlink_properties/mapi_over_http_wlink_client_properties_round_trip_postgresql_table_and_ics_after_reload.md)
- [assert_content_upload_final_state_includes](../../../../../functions/crates/lpe-exchange/src/tests/assert_content_upload_final_state_includes.md)
- [strict_hierarchy_sync_transfer_from_response](../../../../../functions/crates/lpe-exchange/src/tests/strict_hierarchy_sync_transfer_from_response.md)
- [strict_content_sync_transfer_from_response](../../../../../functions/crates/lpe-exchange/src/tests/strict_content_sync_transfer_from_response.md)