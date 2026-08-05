---
type: Rust Function
title: execute_rop_response_summary_skips_implausible_getprops_payload_rop_marker
resource: crates/lpe-exchange/src/mapi/dispatch/tests/execute.rs#L1097-L1125
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/rpc_header_ext_rop_buffer
  - functions/crates/lpe-exchange/src/mapi/rop/errors/rop_buffer_with_response_spec
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/summarize_response_rop_buffer
---

# Signature

`fn execute_rop_response_summary_skips_implausible_getprops_payload_rop_marker()`

# Calls

- [rpc_header_ext_rop_buffer](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/rpc_header_ext_rop_buffer.md)
- [rop_buffer_with_response_spec](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/errors/rop_buffer_with_response_spec.md)
- [summarize_response_rop_buffer](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/summarize_response_rop_buffer.md)