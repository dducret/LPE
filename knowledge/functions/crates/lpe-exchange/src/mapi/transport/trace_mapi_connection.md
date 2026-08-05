---
type: Rust Function
title: trace_mapi_connection
resource: crates/lpe-exchange/src/mapi/transport.rs#L948-L1030
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/transport/cookies/request_cookie
  - functions/crates/lpe-exchange/src/mapi/transport/remote_peer
  - functions/LPE-CT/src/host_logs/HostLogError/status
  - functions/crates/lpe-exchange/src/mapi/transport/execute_request_trace_metadata
  - functions/crates/lpe-exchange/src/mapi/transport/execute_response_trace_metadata
  - functions/crates/lpe-exchange/src/mapi/transport/mapi_response_payload
  - functions/crates/lpe-core/src/outlook_trace/write_outlook_trace
  called_by:
  - functions/crates/lpe-exchange/src/mapi/transport/log_mapi_connection
---

# Signature

`fn trace_mapi_connection( endpoint: MapiEndpoint, principal: &AccountPrincipal, headers: &HeaderMap, request_body: &[u8], request_type: &str, request_id: &str, response: &Response, response_payload_bytes: usize, )`

# Calls

- [request_cookie](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/cookies/request_cookie.md)
- [remote_peer](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/remote_peer.md)
- [status](../../../../../../functions/LPE-CT/src/host_logs/HostLogError/status.md)
- [execute_request_trace_metadata](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/execute_request_trace_metadata.md)
- [execute_response_trace_metadata](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/execute_response_trace_metadata.md)
- [mapi_response_payload](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/mapi_response_payload.md)
- [write_outlook_trace](../../../../../../functions/crates/lpe-core/src/outlook_trace/write_outlook_trace.md)

# Called by

- [log_mapi_connection](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/log_mapi_connection.md)