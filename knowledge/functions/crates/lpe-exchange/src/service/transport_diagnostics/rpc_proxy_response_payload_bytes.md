---
type: Rust Function
title: rpc_proxy_response_payload_bytes
resource: crates/lpe-exchange/src/service/transport_diagnostics.rs#L317-L322
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/service/transport_diagnostics/log_rpc_proxy_connection
---

# Signature

`fn rpc_proxy_response_payload_bytes(response: &Response) -> Option<usize>`

# Called by

- [log_rpc_proxy_connection](../../../../../../functions/crates/lpe-exchange/src/service/transport_diagnostics/log_rpc_proxy_connection.md)