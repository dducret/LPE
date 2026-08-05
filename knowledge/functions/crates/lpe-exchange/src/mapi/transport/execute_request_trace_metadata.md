---
type: Rust Function
title: execute_request_trace_metadata
resource: crates/lpe-exchange/src/mapi/transport.rs#L1109-L1135
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute/parse_execute_request
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/summarize_request_rop_buffer
  called_by:
  - functions/crates/lpe-exchange/src/mapi/transport/trace_mapi_connection
---

# Signature

`fn execute_request_trace_metadata( request_type: &str, request_body: &[u8], ) -> Vec<(&'static str, String)>`

# Calls

- [parse_execute_request](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute/parse_execute_request.md)
- [summarize_request_rop_buffer](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/summarize_request_rop_buffer.md)

# Called by

- [trace_mapi_connection](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/trace_mapi_connection.md)