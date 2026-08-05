---
type: Rust Function
title: log_execute_dispatch_start_debug
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics/execute.rs#L618-L645
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute_response
---

# Signature

`pub(in crate::mapi::dispatch) fn log_execute_dispatch_start_debug( endpoint: MapiEndpoint, principal: &AccountPrincipal, _headers: &HeaderMap, request_id: &str, mailbox_count: usize, email_count: usize, )`

# Called by

- [execute_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute_response.md)