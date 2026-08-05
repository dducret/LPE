---
type: Rust Module
title: transport_diagnostics
resource: crates/lpe-exchange/src/service/transport_diagnostics.rs#L1-L329
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/super
  - external/lpe-core-outlook-trace-write-outlook-trace-outlooktracedirection-outlooktraceevent
  - external/tracing-info-warn
  member_of:
  - packages/crates/lpe-exchange
---

# Contains

- [log_mapi_transport_connection](../../../../../functions/crates/lpe-exchange/src/service/transport_diagnostics/log_mapi_transport_connection.md)
- [log_rpc_proxy_connection](../../../../../functions/crates/lpe-exchange/src/service/transport_diagnostics/log_rpc_proxy_connection.md)
- [trace_rpc_proxy_connection](../../../../../functions/crates/lpe-exchange/src/service/transport_diagnostics/trace_rpc_proxy_connection.md)
- [RpcProxyResponseDebug](../../../../../classes/crates/lpe-exchange/src/service/transport_diagnostics/RpcProxyResponseDebug.md)
- [RpcProxyResponsePayloadPreview](../../../../../classes/crates/lpe-exchange/src/service/transport_diagnostics/RpcProxyResponsePayloadPreview.md)
- [rpc_proxy_response_payload_bytes](../../../../../functions/crates/lpe-exchange/src/service/transport_diagnostics/rpc_proxy_response_payload_bytes.md)
- [rpc_proxy_response_payload_preview_hex](../../../../../functions/crates/lpe-exchange/src/service/transport_diagnostics/rpc_proxy_response_payload_preview_hex.md)

# Imports

- `super::*`
- `lpe_core::outlook_trace::{write_outlook_trace, OutlookTraceDirection, OutlookTraceEvent}`
- `tracing::{info, warn}`

# Member of

- [lpe-exchange](../../../../../packages/crates/lpe-exchange.md)