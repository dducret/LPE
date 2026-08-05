---
type: Rust Function
title: query_parameter
resource: crates/lpe-exchange/src/service/http_utils.rs#L29-L35
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/service/transport_diagnostics/log_mapi_transport_connection
---

# Signature

`pub(super) fn query_parameter(query: &str, name: &str) -> Option<String>`

# Called by

- [log_mapi_transport_connection](../../../../../../functions/crates/lpe-exchange/src/service/transport_diagnostics/log_mapi_transport_connection.md)