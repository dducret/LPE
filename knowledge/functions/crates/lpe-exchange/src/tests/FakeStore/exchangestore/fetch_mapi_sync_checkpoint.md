---
type: Rust Method
title: fetch_mapi_sync_checkpoint
resource: crates/lpe-exchange/src/tests/mod.rs#L7028-L7041
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_configure/append_synchronization_configure_response
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_virtual_calendar_content_sync_stores_virtual_checkpoint
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_advertised_calendar_sync_projects_default_collection_event
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_empty_virtual_calendar_sync_has_no_placeholder_rows
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_outlook_startup_replay_keeps_calendar_search_and_partial_sync_contracts
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/contacts/mapi_over_http_virtual_contacts_content_sync_stores_virtual_checkpoint
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/contacts/mapi_over_http_contacts_search_content_sync_uses_search_folder_parent
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/notifications/mapi_hierarchy_move_and_copy_replay_is_recipient_scoped_and_historical_in_postgresql
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_empty_common_views_observed_outlook_partial_sync_returns_no_fai
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_sync_checkpoint_resumes_incremental_content_with_tombstone
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_hierarchy_sync_client_state_resumes_after_completed_download
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_content_sync_after_empty_folder_advances_empty_final_state
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_sync_source_transfer_state_returns_client_derived_final_state
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

`fn fetch_mapi_sync_checkpoint<'a>( &'a self, _account_id: Uuid, mailbox_id: Option<Uuid>, checkpoint_kind: MapiCheckpointKind, ) -> StoreFuture<'a, Option<MapiSyncCheckpoint>>`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [append_synchronization_configure_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_configure/append_synchronization_configure_response.md)
- [mapi_over_http_virtual_calendar_content_sync_stores_virtual_checkpoint](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_virtual_calendar_content_sync_stores_virtual_checkpoint.md)
- [mapi_over_http_advertised_calendar_sync_projects_default_collection_event](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_advertised_calendar_sync_projects_default_collection_event.md)
- [mapi_over_http_empty_virtual_calendar_sync_has_no_placeholder_rows](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_empty_virtual_calendar_sync_has_no_placeholder_rows.md)
- [mapi_over_http_outlook_startup_replay_keeps_calendar_search_and_partial_sync_contracts](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_outlook_startup_replay_keeps_calendar_search_and_partial_sync_contracts.md)
- [mapi_over_http_virtual_contacts_content_sync_stores_virtual_checkpoint](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/contacts/mapi_over_http_virtual_contacts_content_sync_stores_virtual_checkpoint.md)
- [mapi_over_http_contacts_search_content_sync_uses_search_folder_parent](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/contacts/mapi_over_http_contacts_search_content_sync_uses_search_folder_parent.md)
- [mapi_hierarchy_move_and_copy_replay_is_recipient_scoped_and_historical_in_postgresql](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/notifications/mapi_hierarchy_move_and_copy_replay_is_recipient_scoped_and_historical_in_postgresql.md)
- [mapi_over_http_empty_common_views_observed_outlook_partial_sync_returns_no_fai](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_empty_common_views_observed_outlook_partial_sync_returns_no_fai.md)
- [mapi_over_http_sync_checkpoint_resumes_incremental_content_with_tombstone](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_sync_checkpoint_resumes_incremental_content_with_tombstone.md)
- [mapi_over_http_hierarchy_sync_client_state_resumes_after_completed_download](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_hierarchy_sync_client_state_resumes_after_completed_download.md)
- [mapi_over_http_content_sync_after_empty_folder_advances_empty_final_state](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_content_sync_after_empty_folder_advances_empty_final_state.md)
- [mapi_over_http_sync_source_transfer_state_returns_client_derived_final_state](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_sync_source_transfer_state_returns_client_derived_final_state.md)
- [mapi_over_http_download_transfer_state_handle_cannot_regress_checkpoint](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_download_transfer_state_handle_cannot_regress_checkpoint.md)
- [mapi_over_http_partial_scope_content_sync_does_not_advance_checkpoint](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_partial_scope_content_sync_does_not_advance_checkpoint.md)
- [mapi_over_http_partial_trash_content_scope_does_not_advance_checkpoint](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_partial_trash_content_scope_does_not_advance_checkpoint.md)
- [mapi_over_http_sync_upload_state_returns_server_transfer_state](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_sync_upload_state_returns_server_transfer_state.md)
- [mapi_over_http_upload_import_collector_handles_never_advance_download_checkpoints](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_upload_import_collector_handles_never_advance_download_checkpoints.md)
- [mapi_identity_source_key_lookup_and_checkpoints_round_trip](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_identity_source_key_lookup_and_checkpoints_round_trip.md)
- [postgres_mapi_sync_checkpoint_ignores_and_refreshes_expired_rows](../../../../../../../functions/crates/lpe-exchange/src/tests/postgres_mapi_sync_checkpoint_ignores_and_refreshes_expired_rows.md)
- [mapi_identity_repair_removes_orphaned_checkpoint_and_config_state](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_identity_repair_removes_orphaned_checkpoint_and_config_state.md)