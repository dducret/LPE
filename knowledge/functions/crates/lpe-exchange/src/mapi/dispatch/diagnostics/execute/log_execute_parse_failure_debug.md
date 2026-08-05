---
type: Rust Function
title: log_execute_parse_failure_debug
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics/execute.rs#L647-L704
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/execute/read_le_u32_at
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute_response
---

# Signature

`pub(in crate::mapi::dispatch) fn log_execute_parse_failure_debug( endpoint: MapiEndpoint, principal: &AccountPrincipal, headers: &HeaderMap, request_id: &str, body: &[u8], error: &anyhow::Error, )`

# Calls

- [read_le_u32_at](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/execute/read_le_u32_at.md)

# Called by

- [execute_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute_response.md)