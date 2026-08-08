---
type: Rust Function
title: assert_content_final_state_includes_counters
resource: crates/lpe-exchange/src/tests/mod.rs#L12831-L12853
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/mapi_binary_property_value
  called_by:
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_content_sync_incremental_after_client_state_exports_delta
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_content_sync_move_across_folders_exports_source_tombstone_and_target_change
  - functions/crates/lpe-exchange/src/tests/assert_content_final_state_includes
---

# Signature

`fn assert_content_final_state_includes_counters( bytes: &[u8], message_counters: &[u64], change_numbers: &[u64], )`

# Calls

- [mapi_binary_property_value](../../../../../functions/crates/lpe-exchange/src/tests/mapi_binary_property_value.md)

# Called by

- [mapi_over_http_content_sync_incremental_after_client_state_exports_delta](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_content_sync_incremental_after_client_state_exports_delta.md)
- [mapi_over_http_content_sync_move_across_folders_exports_source_tombstone_and_target_change](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_content_sync_move_across_folders_exports_source_tombstone_and_target_change.md)
- [assert_content_final_state_includes](../../../../../functions/crates/lpe-exchange/src/tests/assert_content_final_state_includes.md)