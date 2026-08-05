---
type: Rust Function
title: guid_counter_debug
resource: crates/lpe-exchange/src/mapi/transport/headers.rs#L78-L90
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/transport/log_mapi_connection
  - functions/crates/lpe-exchange/src/mapi/transport/diagnostics/log_mapi_session_disconnect
  - functions/crates/lpe-exchange/src/mapi/transport/headers/client_flow_key
  - functions/crates/lpe-exchange/src/mapi/transport/notification_wait/notification_wait_response
  - functions/crates/lpe-exchange/src/service/transport_diagnostics/log_mapi_transport_connection
---

# Signature

`pub(crate) fn guid_counter_debug(value: &str) -> (String, String)`

# Called by

- [log_mapi_connection](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/log_mapi_connection.md)
- [log_mapi_session_disconnect](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/diagnostics/log_mapi_session_disconnect.md)
- [client_flow_key](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/headers/client_flow_key.md)
- [notification_wait_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/notification_wait/notification_wait_response.md)
- [log_mapi_transport_connection](../../../../../../../functions/crates/lpe-exchange/src/service/transport_diagnostics/log_mapi_transport_connection.md)