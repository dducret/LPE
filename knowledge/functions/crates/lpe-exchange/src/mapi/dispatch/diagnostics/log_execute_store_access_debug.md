---
type: Rust Function
title: log_execute_store_access_debug
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics.rs#L1350-L1378
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute_response
---

# Signature

`pub(super) fn log_execute_store_access_debug( endpoint: MapiEndpoint, principal: &AccountPrincipal, _headers: &HeaderMap, request_id: &str, access_plan: &MapiAccessPlan, )`

# Called by

- [execute_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute_response.md)