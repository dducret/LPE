---
type: Rust Function
title: log_rpc_proxy_connection
resource: crates/lpe-exchange/src/service/transport_diagnostics.rs#L170-L241
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/host_logs/HostLogError/status
  - functions/crates/lpe-exchange/src/service/transport_diagnostics/rpc_proxy_response_payload_bytes
  - functions/crates/lpe-exchange/src/mapi/transport/headers/debug_payload_preview_hex
  - functions/crates/lpe-exchange/src/service/transport_diagnostics/rpc_proxy_response_payload_preview_hex
  - functions/crates/lpe-exchange/src/service/transport_diagnostics/trace_rpc_proxy_connection
  called_by:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_handler
---

# Signature

`pub(super) fn log_rpc_proxy_connection( method: &Method, uri: &Uri, headers: &HeaderMap, request_body: &[u8], response: &Response, duration_ms: f64, )`

# Calls

- [status](../../../../../../functions/LPE-CT/src/host_logs/HostLogError/status.md)
- [rpc_proxy_response_payload_bytes](../../../../../../functions/crates/lpe-exchange/src/service/transport_diagnostics/rpc_proxy_response_payload_bytes.md)
- [debug_payload_preview_hex](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/headers/debug_payload_preview_hex.md)
- [rpc_proxy_response_payload_preview_hex](../../../../../../functions/crates/lpe-exchange/src/service/transport_diagnostics/rpc_proxy_response_payload_preview_hex.md)
- [trace_rpc_proxy_connection](../../../../../../functions/crates/lpe-exchange/src/service/transport_diagnostics/trace_rpc_proxy_connection.md)

# Called by

- [rpc_proxy_handler](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_handler.md)