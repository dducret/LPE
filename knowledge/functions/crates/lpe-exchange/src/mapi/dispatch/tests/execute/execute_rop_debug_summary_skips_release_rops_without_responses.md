---
type: Rust Function
title: execute_rop_debug_summary_skips_release_rops_without_responses
resource: crates/lpe-exchange/src/mapi/dispatch/tests/execute.rs#L848-L864
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/errors/rop_buffer_with_response
  - functions/crates/lpe-exchange/src/mapi/sync/responses/rop_get_local_replica_ids_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/summarize_response_rop_buffer
---

# Signature

`fn execute_rop_debug_summary_skips_release_rops_without_responses()`

# Calls

- [rop_buffer_with_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/errors/rop_buffer_with_response.md)
- [rop_get_local_replica_ids_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/responses/rop_get_local_replica_ids_response.md)
- [summarize_response_rop_buffer](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/summarize_response_rop_buffer.md)