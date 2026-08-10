---
type: Rust Function
title: assert_content_upload_final_state_includes
resource: crates/lpe-exchange/src/tests/mod.rs#L12860-L12903
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/mapi_fast_transfer_chunks
  - functions/crates/lpe-exchange/src/tests/mapi_binary_property_value
  called_by:
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_outlook_common_views_ics_import_stages_wlinks_until_save
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_sync_upload_state_returns_server_transfer_state
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_upload_import_collector_handles_never_advance_download_checkpoints
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_microsoft_oxcfxics_4_2_1_message_upload_returns_transfer_state
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_microsoft_partial_item_upload_updates_existing_message_without_import
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_sync_upload_state_does_not_echo_multiple_streams
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_replays_outlook_calendar_sync_import_then_save
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_calendar_sync_import_conflict_merges_both_predecessor_lists
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_sync_import_delete_and_read_state_use_canonical_store
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_sync_import_move_uses_canonical_store
---

# Signature

`fn assert_content_upload_final_state_includes( bytes: &[u8], normal_change_numbers: &[u64], associated_change_numbers: &[u64], read_change_numbers: &[u64], )`

# Calls

- [mapi_fast_transfer_chunks](../../../../../functions/crates/lpe-exchange/src/tests/mapi_fast_transfer_chunks.md)
- [mapi_binary_property_value](../../../../../functions/crates/lpe-exchange/src/tests/mapi_binary_property_value.md)

# Called by

- [mapi_over_http_outlook_common_views_ics_import_stages_wlinks_until_save](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_outlook_common_views_ics_import_stages_wlinks_until_save.md)
- [mapi_over_http_sync_upload_state_returns_server_transfer_state](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_sync_upload_state_returns_server_transfer_state.md)
- [mapi_over_http_upload_import_collector_handles_never_advance_download_checkpoints](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_upload_import_collector_handles_never_advance_download_checkpoints.md)
- [mapi_over_http_microsoft_oxcfxics_4_2_1_message_upload_returns_transfer_state](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_microsoft_oxcfxics_4_2_1_message_upload_returns_transfer_state.md)
- [mapi_over_http_microsoft_partial_item_upload_updates_existing_message_without_import](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_microsoft_partial_item_upload_updates_existing_message_without_import.md)
- [mapi_over_http_sync_upload_state_does_not_echo_multiple_streams](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_sync_upload_state_does_not_echo_multiple_streams.md)
- [mapi_over_http_replays_outlook_calendar_sync_import_then_save](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_replays_outlook_calendar_sync_import_then_save.md)
- [mapi_over_http_calendar_sync_import_conflict_merges_both_predecessor_lists](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_calendar_sync_import_conflict_merges_both_predecessor_lists.md)
- [mapi_over_http_sync_import_delete_and_read_state_use_canonical_store](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_sync_import_delete_and_read_state_use_canonical_store.md)
- [mapi_over_http_sync_import_move_uses_canonical_store](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_sync_import_move_uses_canonical_store.md)