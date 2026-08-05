---
type: Rust Function
title: log_mapi_transport_connection
resource: crates/lpe-exchange/src/service/transport_diagnostics.rs#L5-L168
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/host_logs/HostLogError/status
  - functions/crates/lpe-exchange/src/service/http_utils/query_parameter
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-exchange/src/mapi/transport/headers/guid_counter_debug
  - functions/crates/lpe-exchange/src/mapi/transport/headers/client_flow_key
  - functions/crates/lpe-exchange/src/mapi/transport/mapi_response_payload_bytes
  - functions/crates/lpe-exchange/src/mapi/transport/cookies/request_cookie_transport_debug
  called_by:
  - functions/crates/lpe-exchange/src/service/mapi_post_handler
---

# Signature

`pub(super) fn log_mapi_transport_connection( endpoint: MapiEndpoint, uri: &Uri, headers: &HeaderMap, request_body: &[u8], response: &Response, duration_ms: f64, error: Option<&str>, )`

# Calls

- [status](../../../../../../functions/LPE-CT/src/host_logs/HostLogError/status.md)
- [query_parameter](../../../../../../functions/crates/lpe-exchange/src/service/http_utils/query_parameter.md)
- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [guid_counter_debug](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/headers/guid_counter_debug.md)
- [client_flow_key](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/headers/client_flow_key.md)
- [mapi_response_payload_bytes](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/mapi_response_payload_bytes.md)
- [request_cookie_transport_debug](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/cookies/request_cookie_transport_debug.md)

# Called by

- [mapi_post_handler](../../../../../../functions/crates/lpe-exchange/src/service/mapi_post_handler.md)