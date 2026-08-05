---
type: Rust Function
title: log_mapi_connection
resource: crates/lpe-exchange/src/mapi/transport.rs#L822-L946
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/host_logs/HostLogError/status
  - functions/crates/lpe-exchange/src/mapi/transport/mapi_response_payload_bytes
  - functions/crates/lpe-exchange/src/mapi/transport/trace_mapi_connection
  - functions/crates/lpe-exchange/src/mapi/transport/headers/guid_counter_debug
  - functions/crates/lpe-exchange/src/mapi/transport/headers/client_flow_key
  called_by:
  - functions/crates/lpe-exchange/src/mapi/transport/handle_mapi
  - functions/crates/lpe-exchange/src/mapi/transport/notification_wait/complete_notification_wait
---

# Signature

`pub(in crate::mapi) fn log_mapi_connection( endpoint: MapiEndpoint, principal: &AccountPrincipal, headers: &HeaderMap, request_body: &[u8], request_type: &str, request_id: &str, response: &Response, )`

# Calls

- [status](../../../../../../functions/LPE-CT/src/host_logs/HostLogError/status.md)
- [mapi_response_payload_bytes](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/mapi_response_payload_bytes.md)
- [trace_mapi_connection](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/trace_mapi_connection.md)
- [guid_counter_debug](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/headers/guid_counter_debug.md)
- [client_flow_key](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/headers/client_flow_key.md)

# Called by

- [handle_mapi](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/handle_mapi.md)
- [complete_notification_wait](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/notification_wait/complete_notification_wait.md)