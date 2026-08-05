---
type: Rust Function
title: response_handle_table
resource: crates/lpe-exchange/src/mapi/session.rs#L1395-L1412
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/session/response_handle_table_with_released_handle_sentinel
  - functions/crates/lpe-exchange/src/mapi/session/tests/response_handle_table_preserves_sparse_output_handle_indexes
  - functions/crates/lpe-exchange/src/mapi/session/tests/response_handle_table_can_echo_released_input_slots
---

# Signature

`pub(in crate::mapi) fn response_handle_table( handle_slots: &[u32], output_handles: &[u32], echo_input_handles: bool, ) -> Vec<u32>`

# Called by

- [response_handle_table_with_released_handle_sentinel](../../../../../../functions/crates/lpe-exchange/src/mapi/session/response_handle_table_with_released_handle_sentinel.md)
- [response_handle_table_preserves_sparse_output_handle_indexes](../../../../../../functions/crates/lpe-exchange/src/mapi/session/tests/response_handle_table_preserves_sparse_output_handle_indexes.md)
- [response_handle_table_can_echo_released_input_slots](../../../../../../functions/crates/lpe-exchange/src/mapi/session/tests/response_handle_table_can_echo_released_input_slots.md)