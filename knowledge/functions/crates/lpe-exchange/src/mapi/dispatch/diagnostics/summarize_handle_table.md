---
type: Rust Function
title: summarize_handle_table
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics.rs#L358-L386
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/session/read_handle_table
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/summarize_request_rop_buffer
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/summarize_response_rop_buffer_with_optional_expected_handles
---

# Signature

`pub(super) fn summarize_handle_table( handle_table: &[u8], parse_error: &mut String, ) -> (usize, String)`

# Calls

- [read_handle_table](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/read_handle_table.md)

# Called by

- [summarize_request_rop_buffer](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/summarize_request_rop_buffer.md)
- [summarize_response_rop_buffer_with_optional_expected_handles](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/summarize_response_rop_buffer_with_optional_expected_handles.md)