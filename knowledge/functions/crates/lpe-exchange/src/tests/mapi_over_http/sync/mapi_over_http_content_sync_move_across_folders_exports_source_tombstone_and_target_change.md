---
type: Rust Function
title: mapi_over_http_content_sync_move_across_folders_exports_source_tombstone_and_target_change
resource: crates/lpe-exchange/src/tests/mapi_over_http/sync.rs#L6031-L6105
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/store_mapi_sync_checkpoint
  - functions/crates/lpe-exchange/src/tests/mapi_message_global_counter
  - functions/crates/lpe-exchange/src/tests/outlook_content_sync_state_properties
  - functions/crates/lpe-exchange/src/tests/outlook_content_sync_response_rops_for_store
  - functions/crates/lpe-exchange/src/tests/assert_content_final_state_includes
  - functions/crates/lpe-exchange/src/tests/strict_content_sync_transfer_from_response
  - functions/crates/lpe-exchange/src/tests/assert_content_final_state_includes_counters
---

# Signature

`async fn mapi_over_http_content_sync_move_across_folders_exports_source_tombstone_and_target_change( )`

# Calls

- [store_mapi_sync_checkpoint](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/store_mapi_sync_checkpoint.md)
- [mapi_message_global_counter](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_message_global_counter.md)
- [outlook_content_sync_state_properties](../../../../../../../functions/crates/lpe-exchange/src/tests/outlook_content_sync_state_properties.md)
- [outlook_content_sync_response_rops_for_store](../../../../../../../functions/crates/lpe-exchange/src/tests/outlook_content_sync_response_rops_for_store.md)
- [assert_content_final_state_includes](../../../../../../../functions/crates/lpe-exchange/src/tests/assert_content_final_state_includes.md)
- [strict_content_sync_transfer_from_response](../../../../../../../functions/crates/lpe-exchange/src/tests/strict_content_sync_transfer_from_response.md)
- [assert_content_final_state_includes_counters](../../../../../../../functions/crates/lpe-exchange/src/tests/assert_content_final_state_includes_counters.md)