---
type: Rust Function
title: load_mapi_identity_codec_for_test
resource: crates/lpe-exchange/src/mapi/store_adapter.rs#L85-L93
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/store_adapter/load_mapi_identity_scope
  called_by:
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/with_scoped_mapi_identity
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_selective_reopen_uses_durable_event_modseq
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_default_entry_id_converts_to_openable_folder_id
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_virtual_calendar_content_sync_stores_virtual_checkpoint
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_move_copy_messages_uses_canonical_store
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_depth_root_hierarchy_table_delivers_informative_folder_rows
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_fast_transfer_get_buffer_resumes_across_execute_requests
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/contacts/mapi_over_http_replays_outlook_contact_sync_import_then_save
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/hierarchy/mapi_over_http_empty_store_root_and_ipm_subtree_report_virtual_children
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/hierarchy/mapi_over_http_root_hierarchy_findrow_finds_ipm_subtree_by_display_name
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/hierarchy/mapi_over_http_root_hierarchy_findrow_finds_ipm_subtree_by_entry_id
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/hierarchy/mapi_over_http_microsoft_oxcfold_create_delete_and_move_use_canonical_mailboxes
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/hierarchy/mapi_over_http_move_folder_rejects_wrong_source_parent_without_side_effects
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/hierarchy/mapi_over_http_microsoft_folder_move_accepts_nonzero_boolean_fields_and_copy_rejects
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_content_sync_incremental_after_client_state_exports_delta
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_message_list_settings_import_preserves_outlook_identity_and_content
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/tables/mapi_over_http_hierarchy_table_includes_default_ipm_special_folders
  - functions/crates/lpe-exchange/src/tests/durable_special_folder_id_for_test
---

# Signature

`pub(crate) async fn load_mapi_identity_codec_for_test<S>( store: &S, account_id: Uuid, ) -> Result<crate::mapi::identity::MapiIdentityCodec> where S: ExchangeStore,`

# Calls

- [load_mapi_identity_scope](../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/load_mapi_identity_scope.md)

# Called by

- [with_scoped_mapi_identity](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/with_scoped_mapi_identity.md)
- [mapi_over_http_calendar_selective_reopen_uses_durable_event_modseq](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_selective_reopen_uses_durable_event_modseq.md)
- [mapi_over_http_calendar_default_entry_id_converts_to_openable_folder_id](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_default_entry_id_converts_to_openable_folder_id.md)
- [mapi_over_http_virtual_calendar_content_sync_stores_virtual_checkpoint](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_virtual_calendar_content_sync_stores_virtual_checkpoint.md)
- [mapi_over_http_move_copy_messages_uses_canonical_store](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_move_copy_messages_uses_canonical_store.md)
- [mapi_over_http_depth_root_hierarchy_table_delivers_informative_folder_rows](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_depth_root_hierarchy_table_delivers_informative_folder_rows.md)
- [mapi_over_http_fast_transfer_get_buffer_resumes_across_execute_requests](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_fast_transfer_get_buffer_resumes_across_execute_requests.md)
- [mapi_over_http_replays_outlook_contact_sync_import_then_save](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/contacts/mapi_over_http_replays_outlook_contact_sync_import_then_save.md)
- [mapi_over_http_empty_store_root_and_ipm_subtree_report_virtual_children](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/hierarchy/mapi_over_http_empty_store_root_and_ipm_subtree_report_virtual_children.md)
- [mapi_over_http_root_hierarchy_findrow_finds_ipm_subtree_by_display_name](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/hierarchy/mapi_over_http_root_hierarchy_findrow_finds_ipm_subtree_by_display_name.md)
- [mapi_over_http_root_hierarchy_findrow_finds_ipm_subtree_by_entry_id](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/hierarchy/mapi_over_http_root_hierarchy_findrow_finds_ipm_subtree_by_entry_id.md)
- [mapi_over_http_microsoft_oxcfold_create_delete_and_move_use_canonical_mailboxes](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/hierarchy/mapi_over_http_microsoft_oxcfold_create_delete_and_move_use_canonical_mailboxes.md)
- [mapi_over_http_move_folder_rejects_wrong_source_parent_without_side_effects](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/hierarchy/mapi_over_http_move_folder_rejects_wrong_source_parent_without_side_effects.md)
- [mapi_over_http_microsoft_folder_move_accepts_nonzero_boolean_fields_and_copy_rejects](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/hierarchy/mapi_over_http_microsoft_folder_move_accepts_nonzero_boolean_fields_and_copy_rejects.md)
- [mapi_over_http_content_sync_incremental_after_client_state_exports_delta](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_content_sync_incremental_after_client_state_exports_delta.md)
- [mapi_over_http_message_list_settings_import_preserves_outlook_identity_and_content](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_message_list_settings_import_preserves_outlook_identity_and_content.md)
- [mapi_over_http_hierarchy_table_includes_default_ipm_special_folders](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/tables/mapi_over_http_hierarchy_table_includes_default_ipm_special_folders.md)
- [durable_special_folder_id_for_test](../../../../../../functions/crates/lpe-exchange/src/tests/durable_special_folder_id_for_test.md)