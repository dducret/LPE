---
type: Rust Function
title: rpc_proxy_response_payload_preview_hex
resource: crates/lpe-exchange/src/service/transport_diagnostics.rs#L324-L329
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/service/transport_diagnostics/log_rpc_proxy_connection
---

# Signature

`fn rpc_proxy_response_payload_preview_hex(response: &Response) -> Option<&str>`

# Called by

- [log_rpc_proxy_connection](../../../../../../functions/crates/lpe-exchange/src/service/transport_diagnostics/log_rpc_proxy_connection.md)