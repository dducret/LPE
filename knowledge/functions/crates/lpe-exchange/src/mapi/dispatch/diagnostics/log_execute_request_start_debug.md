---
type: Rust Function
title: log_execute_request_start_debug
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics.rs#L1290-L1348
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute_response
---

# Signature

`pub(super) fn log_execute_request_start_debug( endpoint: MapiEndpoint, principal: &AccountPrincipal, headers: &HeaderMap, request_id: &str, request_body_bytes: usize, request_rop_buffer: &[u8], request: &RopRequestDebugSummary, )`

# Called by

- [execute_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute_response.md)