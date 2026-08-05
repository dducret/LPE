---
type: Rust Function
title: parse_execute_request
resource: crates/lpe-exchange/src/mapi/dispatch/execute.rs#L13-L26
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_bytes
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/parse_execute_request_keeps_max_rop_out
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/parse_execute_request_preserves_chain_flag
  - functions/crates/lpe-exchange/src/mapi/transport/execute_response_trace_metadata
  - functions/crates/lpe-exchange/src/mapi/transport/execute_request_trace_metadata
---

# Signature

`pub(in crate::mapi) fn parse_execute_request(body: &[u8]) -> Result<ExecuteRequest>`

# Calls

- [read_bytes](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_bytes.md)

# Called by

- [execute_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute_response.md)
- [parse_execute_request_keeps_max_rop_out](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/parse_execute_request_keeps_max_rop_out.md)
- [parse_execute_request_preserves_chain_flag](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/parse_execute_request_preserves_chain_flag.md)
- [execute_response_trace_metadata](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/execute_response_trace_metadata.md)
- [execute_request_trace_metadata](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/execute_request_trace_metadata.md)