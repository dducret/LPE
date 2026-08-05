---
type: Rust Function
title: log_connect_body_debug
resource: crates/lpe-exchange/src/mapi/transport/diagnostics.rs#L20-L53
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/transport/diagnostics/summarize_connect_body
  called_by:
  - functions/crates/lpe-exchange/src/mapi/transport/connect_response
---

# Signature

`pub(in crate::mapi) fn log_connect_body_debug( endpoint: MapiEndpoint, principal: &AccountPrincipal, request_id: &str, body: &[u8], )`

# Calls

- [summarize_connect_body](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/diagnostics/summarize_connect_body.md)

# Called by

- [connect_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/connect_response.md)