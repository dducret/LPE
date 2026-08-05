---
type: Rust Function
title: execute_response_trace_metadata
resource: crates/lpe-exchange/src/mapi/transport.rs#L1032-L1079
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute/parse_execute_request
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/summarize_request_rop_buffer
  - functions/crates/lpe-exchange/src/mapi/transport/execute_response_rop_buffer_for_trace
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/summarize_response_rop_buffer_with_expected_handles
  called_by:
  - functions/crates/lpe-exchange/src/mapi/transport/trace_mapi_connection
  - functions/crates/lpe-exchange/src/mapi/transport/tests/execute_response_trace_metadata_summarizes_response_rops
  - functions/crates/lpe-exchange/src/mapi/transport/tests/execute_response_trace_metadata_summarizes_mixed_multi_rop_execute
---

# Signature

`fn execute_response_trace_metadata( request_type: &str, request_body: &[u8], response_body: &[u8], ) -> Vec<(&'static str, String)>`

# Calls

- [parse_execute_request](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute/parse_execute_request.md)
- [summarize_request_rop_buffer](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/summarize_request_rop_buffer.md)
- [execute_response_rop_buffer_for_trace](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/execute_response_rop_buffer_for_trace.md)
- [summarize_response_rop_buffer_with_expected_handles](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/summarize_response_rop_buffer_with_expected_handles.md)

# Called by

- [trace_mapi_connection](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/trace_mapi_connection.md)
- [execute_response_trace_metadata_summarizes_response_rops](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/tests/execute_response_trace_metadata_summarizes_response_rops.md)
- [execute_response_trace_metadata_summarizes_mixed_multi_rop_execute](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/tests/execute_response_trace_metadata_summarizes_mixed_multi_rop_execute.md)