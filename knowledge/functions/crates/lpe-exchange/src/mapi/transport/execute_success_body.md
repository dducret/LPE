---
type: Rust Function
title: execute_success_body
resource: crates/lpe-exchange/src/mapi/transport.rs#L1144-L1157
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute_response
  - functions/crates/lpe-exchange/src/mapi/transport/tests/execute_response_trace_metadata_summarizes_response_rops
  - functions/crates/lpe-exchange/src/mapi/transport/tests/execute_response_trace_metadata_summarizes_mixed_multi_rop_execute
---

# Signature

`pub(in crate::mapi) fn execute_success_body( rop_buffer: Vec<u8>, auxiliary_buffer: Vec<u8>, ) -> Vec<u8>`

# Called by

- [execute_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute_response.md)
- [execute_response_trace_metadata_summarizes_response_rops](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/tests/execute_response_trace_metadata_summarizes_response_rops.md)
- [execute_response_trace_metadata_summarizes_mixed_multi_rop_execute](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/tests/execute_response_trace_metadata_summarizes_mixed_multi_rop_execute.md)