---
type: Rust Function
title: assert_content_final_state_includes
resource: crates/lpe-exchange/src/tests/mod.rs#L12828-L12834
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/assert_content_final_state_includes_counters
  called_by:
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_content_sync_first_baseline_exports_all_current_messages
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_content_sync_move_across_folders_exports_source_tombstone_and_target_change
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_content_sync_hard_delete_exports_tombstone_and_empty_final_state
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_content_sync_read_flag_update_exports_message_change_without_read_state
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_content_sync_incremental_does_not_leak_protected_bcc
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_content_sync_after_empty_folder_advances_empty_final_state
---

# Signature

`fn assert_content_final_state_includes(bytes: &[u8], message_ids: &[Uuid], change_numbers: &[u64])`

# Calls

- [assert_content_final_state_includes_counters](../../../../../functions/crates/lpe-exchange/src/tests/assert_content_final_state_includes_counters.md)

# Called by

- [mapi_over_http_content_sync_first_baseline_exports_all_current_messages](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_content_sync_first_baseline_exports_all_current_messages.md)
- [mapi_over_http_content_sync_move_across_folders_exports_source_tombstone_and_target_change](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_content_sync_move_across_folders_exports_source_tombstone_and_target_change.md)
- [mapi_over_http_content_sync_hard_delete_exports_tombstone_and_empty_final_state](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_content_sync_hard_delete_exports_tombstone_and_empty_final_state.md)
- [mapi_over_http_content_sync_read_flag_update_exports_message_change_without_read_state](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_content_sync_read_flag_update_exports_message_change_without_read_state.md)
- [mapi_over_http_content_sync_incremental_does_not_leak_protected_bcc](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_content_sync_incremental_does_not_leak_protected_bcc.md)
- [mapi_over_http_content_sync_after_empty_folder_advances_empty_final_state](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_content_sync_after_empty_folder_advances_empty_final_state.md)