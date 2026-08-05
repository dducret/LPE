---
type: Rust Function
title: mapi_response_payload_bytes
resource: crates/lpe-exchange/src/mapi/transport.rs#L786-L791
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/transport/log_mapi_connection
  - functions/crates/lpe-exchange/src/service/transport_diagnostics/log_mapi_transport_connection
---

# Signature

`pub(crate) fn mapi_response_payload_bytes(response: &Response) -> Option<usize>`

# Called by

- [log_mapi_connection](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/log_mapi_connection.md)
- [log_mapi_transport_connection](../../../../../../functions/crates/lpe-exchange/src/service/transport_diagnostics/log_mapi_transport_connection.md)