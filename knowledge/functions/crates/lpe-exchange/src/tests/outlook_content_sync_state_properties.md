---
type: Rust Function
title: outlook_content_sync_state_properties
resource: crates/lpe-exchange/src/tests/mod.rs#L13631-L13655
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_conversation_action_content_sync_exports_deletes
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_associated_config_content_sync_exports_deletes
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_associated_config_delete_does_not_allocate_identity
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_sync_checkpoint_resumes_incremental_content_with_tombstone
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_content_sync_uses_retired_move_mid_for_source_tombstone
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_microsoft_oxcfxics_4_5_content_sync_stream_shape
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_content_sync_incremental_after_client_state_exports_delta
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_content_sync_move_across_folders_exports_source_tombstone_and_target_change
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_content_sync_hard_delete_exports_tombstone_and_empty_final_state
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_content_sync_read_flag_update_exports_message_change_without_read_state
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_content_sync_incremental_does_not_leak_protected_bcc
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_content_sync_after_empty_folder_advances_empty_final_state
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_sync_source_transfer_state_returns_client_derived_final_state
---

# Signature

`fn outlook_content_sync_state_properties( object_counters: &[u64], normal_change_numbers: &[u64], fai_change_numbers: &[u64], read_change_numbers: &[u64], ) -> Vec<(u32, Vec<u8>)>`

# Called by

- [mapi_over_http_conversation_action_content_sync_exports_deletes](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_conversation_action_content_sync_exports_deletes.md)
- [mapi_over_http_associated_config_content_sync_exports_deletes](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_associated_config_content_sync_exports_deletes.md)
- [mapi_over_http_associated_config_delete_does_not_allocate_identity](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_associated_config_delete_does_not_allocate_identity.md)
- [mapi_over_http_sync_checkpoint_resumes_incremental_content_with_tombstone](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_sync_checkpoint_resumes_incremental_content_with_tombstone.md)
- [mapi_over_http_content_sync_uses_retired_move_mid_for_source_tombstone](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_content_sync_uses_retired_move_mid_for_source_tombstone.md)
- [mapi_over_http_microsoft_oxcfxics_4_5_content_sync_stream_shape](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_microsoft_oxcfxics_4_5_content_sync_stream_shape.md)
- [mapi_over_http_content_sync_incremental_after_client_state_exports_delta](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_content_sync_incremental_after_client_state_exports_delta.md)
- [mapi_over_http_content_sync_move_across_folders_exports_source_tombstone_and_target_change](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_content_sync_move_across_folders_exports_source_tombstone_and_target_change.md)
- [mapi_over_http_content_sync_hard_delete_exports_tombstone_and_empty_final_state](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_content_sync_hard_delete_exports_tombstone_and_empty_final_state.md)
- [mapi_over_http_content_sync_read_flag_update_exports_message_change_without_read_state](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_content_sync_read_flag_update_exports_message_change_without_read_state.md)
- [mapi_over_http_content_sync_incremental_does_not_leak_protected_bcc](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_content_sync_incremental_does_not_leak_protected_bcc.md)
- [mapi_over_http_content_sync_after_empty_folder_advances_empty_final_state](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_content_sync_after_empty_folder_advances_empty_final_state.md)
- [mapi_over_http_sync_source_transfer_state_returns_client_derived_final_state](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_sync_source_transfer_state_returns_client_derived_final_state.md)