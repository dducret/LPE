---
type: Rust Method
title: store_mapi_sync_checkpoint
resource: crates/lpe-exchange/src/tests/mod.rs#L7112-L7145
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_get_buffer/append_fast_transfer_source_get_buffer_response
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_move_to_deleted_items_rekeys_and_projects_canonical_event
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_fai_only_sync_does_not_project_synthetic_configuration
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_outlook_startup_replay_keeps_calendar_search_and_partial_sync_contracts
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_ics_transient_read_state_uses_message_changes_not_synthetic_read_sets
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_ics_client_state_controls_baseline_versus_delta_selection
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/contacts/mapi_over_http_contacts_sync_exports_associated_config_deletes
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/contacts/mapi_over_http_contact_content_sync_exports_deletes
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/notifications/mapi_hierarchy_move_and_copy_replay_is_recipient_scoped_and_historical_in_postgresql
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_conversation_action_content_sync_exports_deletes
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_associated_config_content_sync_exports_deletes
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_associated_config_delete_does_not_allocate_identity
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_sync_checkpoint_resumes_incremental_content_with_tombstone
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_microsoft_oxcfxics_4_5_content_sync_stream_shape
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_content_sync_incremental_after_client_state_exports_delta
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_content_sync_move_across_folders_exports_source_tombstone_and_target_change
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_content_sync_hard_delete_exports_tombstone_and_empty_final_state
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_content_sync_read_flag_update_exports_message_change_without_read_state
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_content_sync_incremental_does_not_leak_protected_bcc
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_hierarchy_sync_replays_version_2_server_checkpoint
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_hierarchy_sync_uses_baseline_for_stale_root_checkpoint_with_client_state
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_content_sync_after_empty_folder_advances_empty_final_state
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_download_transfer_state_handle_cannot_regress_checkpoint
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_partial_scope_content_sync_does_not_advance_checkpoint
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_partial_trash_content_scope_does_not_advance_checkpoint
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_sync_upload_state_returns_server_transfer_state
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_upload_import_collector_handles_never_advance_download_checkpoints
  - functions/crates/lpe-exchange/src/tests/mapi_identity_source_key_lookup_and_checkpoints_round_trip
  - functions/crates/lpe-exchange/src/tests/postgres_mapi_sync_checkpoint_ignores_and_refreshes_expired_rows
  - functions/crates/lpe-exchange/src/tests/mapi_identity_repair_removes_orphaned_checkpoint_and_config_state
---

# Signature

`fn store_mapi_sync_checkpoint<'a>( &'a self, _account_id: Uuid, mailbox_id: Option<Uuid>, checkpoint_kind: MapiCheckpointKind, last_change_sequence: u64, last_modseq: u64, cursor_json: serde_json::Value, ) -> StoreFuture<'a, MapiSyncCheckpoint>`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [append_fast_transfer_source_get_buffer_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_get_buffer/append_fast_transfer_source_get_buffer_response.md)
- [mapi_over_http_calendar_move_to_deleted_items_rekeys_and_projects_canonical_event](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_move_to_deleted_items_rekeys_and_projects_canonical_event.md)
- [mapi_over_http_calendar_fai_only_sync_does_not_project_synthetic_configuration](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_fai_only_sync_does_not_project_synthetic_configuration.md)
- [mapi_over_http_outlook_startup_replay_keeps_calendar_search_and_partial_sync_contracts](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_outlook_startup_replay_keeps_calendar_search_and_partial_sync_contracts.md)
- [mapi_over_http_ics_transient_read_state_uses_message_changes_not_synthetic_read_sets](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_ics_transient_read_state_uses_message_changes_not_synthetic_read_sets.md)
- [mapi_over_http_ics_client_state_controls_baseline_versus_delta_selection](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_ics_client_state_controls_baseline_versus_delta_selection.md)
- [mapi_over_http_contacts_sync_exports_associated_config_deletes](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/contacts/mapi_over_http_contacts_sync_exports_associated_config_deletes.md)
- [mapi_over_http_contact_content_sync_exports_deletes](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/contacts/mapi_over_http_contact_content_sync_exports_deletes.md)
- [mapi_hierarchy_move_and_copy_replay_is_recipient_scoped_and_historical_in_postgresql](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/notifications/mapi_hierarchy_move_and_copy_replay_is_recipient_scoped_and_historical_in_postgresql.md)
- [mapi_over_http_conversation_action_content_sync_exports_deletes](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_conversation_action_content_sync_exports_deletes.md)
- [mapi_over_http_associated_config_content_sync_exports_deletes](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_associated_config_content_sync_exports_deletes.md)
- [mapi_over_http_associated_config_delete_does_not_allocate_identity](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_associated_config_delete_does_not_allocate_identity.md)
- [mapi_over_http_sync_checkpoint_resumes_incremental_content_with_tombstone](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_sync_checkpoint_resumes_incremental_content_with_tombstone.md)
- [mapi_over_http_microsoft_oxcfxics_4_5_content_sync_stream_shape](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_microsoft_oxcfxics_4_5_content_sync_stream_shape.md)
- [mapi_over_http_content_sync_incremental_after_client_state_exports_delta](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_content_sync_incremental_after_client_state_exports_delta.md)
- [mapi_over_http_content_sync_move_across_folders_exports_source_tombstone_and_target_change](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_content_sync_move_across_folders_exports_source_tombstone_and_target_change.md)
- [mapi_over_http_content_sync_hard_delete_exports_tombstone_and_empty_final_state](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_content_sync_hard_delete_exports_tombstone_and_empty_final_state.md)
- [mapi_over_http_content_sync_read_flag_update_exports_message_change_without_read_state](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_content_sync_read_flag_update_exports_message_change_without_read_state.md)
- [mapi_over_http_content_sync_incremental_does_not_leak_protected_bcc](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_content_sync_incremental_does_not_leak_protected_bcc.md)
- [mapi_over_http_hierarchy_sync_replays_version_2_server_checkpoint](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_hierarchy_sync_replays_version_2_server_checkpoint.md)
- [mapi_over_http_hierarchy_sync_uses_baseline_for_stale_root_checkpoint_with_client_state](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_hierarchy_sync_uses_baseline_for_stale_root_checkpoint_with_client_state.md)
- [mapi_over_http_content_sync_after_empty_folder_advances_empty_final_state](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_content_sync_after_empty_folder_advances_empty_final_state.md)
- [mapi_over_http_download_transfer_state_handle_cannot_regress_checkpoint](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_download_transfer_state_handle_cannot_regress_checkpoint.md)
- [mapi_over_http_partial_scope_content_sync_does_not_advance_checkpoint](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_partial_scope_content_sync_does_not_advance_checkpoint.md)
- [mapi_over_http_partial_trash_content_scope_does_not_advance_checkpoint](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_partial_trash_content_scope_does_not_advance_checkpoint.md)
- [mapi_over_http_sync_upload_state_returns_server_transfer_state](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_sync_upload_state_returns_server_transfer_state.md)
- [mapi_over_http_upload_import_collector_handles_never_advance_download_checkpoints](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_upload_import_collector_handles_never_advance_download_checkpoints.md)
- [mapi_identity_source_key_lookup_and_checkpoints_round_trip](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_identity_source_key_lookup_and_checkpoints_round_trip.md)
- [postgres_mapi_sync_checkpoint_ignores_and_refreshes_expired_rows](../../../../../../../functions/crates/lpe-exchange/src/tests/postgres_mapi_sync_checkpoint_ignores_and_refreshes_expired_rows.md)
- [mapi_identity_repair_removes_orphaned_checkpoint_and_config_state](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_identity_repair_removes_orphaned_checkpoint_and_config_state.md)