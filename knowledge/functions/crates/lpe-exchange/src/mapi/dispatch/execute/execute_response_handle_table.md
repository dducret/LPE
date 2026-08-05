---
type: Rust Function
title: execute_response_handle_table
resource: crates/lpe-exchange/src/mapi/dispatch/execute.rs#L246-L279
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/session/response_handle_table_with_released_handle_sentinel
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute_rops
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute/finalize_execute_rop_buffer
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/release_only_execute_response_uses_exchange_released_handle_sentinel
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/release_with_appended_notification_uses_exchange_released_handle_sentinel
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/mixed_release_execute_response_preserves_sparse_output_handle_index
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/mixed_create_save_batch_preserves_save_response_folder_handle_slot
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/mixed_setcolumns_release_response_omits_release_only_handle_slots
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/mixed_setcolumns_release_response_trims_snapshot_to_response_handle_index
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/mixed_setcolumns_trailing_release_returns_invalid_released_handle
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/outlook_setcolumns_then_release_same_slot_returns_post_release_handle_table
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/non_release_echo_response_keeps_output_placeholders
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/mixed_release_response_keeps_unreleased_sparse_output_holes
---

# Signature

`pub(super) fn execute_response_handle_table( responses: &[u8], handle_slots: &[u32], output_handles: &[u32], response_handle_indexes: &[u8], echo_input_handle_table: bool, released_handle_indexes: &[u8], ) -> Vec<u32>`

# Calls

- [response_handle_table_with_released_handle_sentinel](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/response_handle_table_with_released_handle_sentinel.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [execute_rops](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute_rops.md)
- [finalize_execute_rop_buffer](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute/finalize_execute_rop_buffer.md)
- [release_only_execute_response_uses_exchange_released_handle_sentinel](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/release_only_execute_response_uses_exchange_released_handle_sentinel.md)
- [release_with_appended_notification_uses_exchange_released_handle_sentinel](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/release_with_appended_notification_uses_exchange_released_handle_sentinel.md)
- [mixed_release_execute_response_preserves_sparse_output_handle_index](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/mixed_release_execute_response_preserves_sparse_output_handle_index.md)
- [mixed_create_save_batch_preserves_save_response_folder_handle_slot](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/mixed_create_save_batch_preserves_save_response_folder_handle_slot.md)
- [mixed_setcolumns_release_response_omits_release_only_handle_slots](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/mixed_setcolumns_release_response_omits_release_only_handle_slots.md)
- [mixed_setcolumns_release_response_trims_snapshot_to_response_handle_index](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/mixed_setcolumns_release_response_trims_snapshot_to_response_handle_index.md)
- [mixed_setcolumns_trailing_release_returns_invalid_released_handle](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/mixed_setcolumns_trailing_release_returns_invalid_released_handle.md)
- [outlook_setcolumns_then_release_same_slot_returns_post_release_handle_table](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/outlook_setcolumns_then_release_same_slot_returns_post_release_handle_table.md)
- [non_release_echo_response_keeps_output_placeholders](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/non_release_echo_response_keeps_output_placeholders.md)
- [mixed_release_response_keeps_unreleased_sparse_output_holes](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/mixed_release_response_keeps_unreleased_sparse_output_holes.md)