---
type: Rust Function
title: execute_response_rop_buffer_for_trace
resource: crates/lpe-exchange/src/mapi/transport.rs#L1081-L1107
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/transport/execute_response_trace_metadata
---

# Signature

`fn execute_response_rop_buffer_for_trace(response_body: &[u8]) -> Result<&[u8], String>`

# Calls

- [get](../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [execute_response_trace_metadata](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/execute_response_trace_metadata.md)