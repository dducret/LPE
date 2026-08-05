---
type: Rust Function
title: trace_rpc_proxy_connection
resource: crates/lpe-exchange/src/service/transport_diagnostics.rs#L243-L305
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-core/src/sieve/Parser/next
  - functions/crates/lpe-core/src/outlook_trace/write_outlook_trace
  - functions/LPE-CT/src/host_logs/HostLogError/status
  called_by:
  - functions/crates/lpe-exchange/src/service/transport_diagnostics/log_rpc_proxy_connection
---

# Signature

`fn trace_rpc_proxy_connection( method: &Method, uri: &Uri, headers: &HeaderMap, request_body: &[u8], response: &Response, response_kind: &str, response_payload_bytes: usize, )`

# Calls

- [next](../../../../../../functions/crates/lpe-core/src/sieve/Parser/next.md)
- [write_outlook_trace](../../../../../../functions/crates/lpe-core/src/outlook_trace/write_outlook_trace.md)
- [status](../../../../../../functions/LPE-CT/src/host_logs/HostLogError/status.md)

# Called by

- [log_rpc_proxy_connection](../../../../../../functions/crates/lpe-exchange/src/service/transport_diagnostics/log_rpc_proxy_connection.md)