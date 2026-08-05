---
type: Rust Function
title: is_rpc_header_ext_rop_buffer
resource: crates/lpe-exchange/src/mapi/rop/buffer.rs#L203-L205
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/rpc_header_ext_payload
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/summarize_request_rop_buffer
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/summarize_response_rop_buffer_with_optional_expected_handles
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/rop_buffer_layout_name
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute/apply_execute_max_rop_out
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute/parse_execute_rop_dispatch_input
---

# Signature

`pub(in crate::mapi) fn is_rpc_header_ext_rop_buffer(buffer: &[u8]) -> bool`

# Calls

- [rpc_header_ext_payload](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/rpc_header_ext_payload.md)

# Called by

- [summarize_request_rop_buffer](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/summarize_request_rop_buffer.md)
- [summarize_response_rop_buffer_with_optional_expected_handles](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/summarize_response_rop_buffer_with_optional_expected_handles.md)
- [rop_buffer_layout_name](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/rop_buffer_layout_name.md)
- [apply_execute_max_rop_out](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute/apply_execute_max_rop_out.md)
- [parse_execute_rop_dispatch_input](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute/parse_execute_rop_dispatch_input.md)