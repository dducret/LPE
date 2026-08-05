---
type: Rust Function
title: response_handle_table_with_released_handle_sentinel
resource: crates/lpe-exchange/src/mapi/session.rs#L1414-L1433
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/session/response_handle_table
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute/execute_response_handle_table
---

# Signature

`pub(in crate::mapi) fn response_handle_table_with_released_handle_sentinel( handle_slots: &[u32], output_handles: &[u32], echo_input_handles: bool, released_handle_indexes: &[u8], ) -> Vec<u32>`

# Calls

- [response_handle_table](../../../../../../functions/crates/lpe-exchange/src/mapi/session/response_handle_table.md)

# Called by

- [execute_response_handle_table](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute/execute_response_handle_table.md)