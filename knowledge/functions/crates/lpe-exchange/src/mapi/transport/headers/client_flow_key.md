---
type: Rust Function
title: client_flow_key
resource: crates/lpe-exchange/src/mapi/transport/headers.rs#L92-L99
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/transport/headers/guid_counter_debug
  called_by:
  - functions/crates/lpe-exchange/src/mapi/transport/log_mapi_connection
  - functions/crates/lpe-exchange/src/mapi/transport/diagnostics/log_mapi_session_disconnect
  - functions/crates/lpe-exchange/src/service/transport_diagnostics/log_mapi_transport_connection
---

# Signature

`pub(crate) fn client_flow_key(client_info: &str) -> String`

# Calls

- [guid_counter_debug](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/headers/guid_counter_debug.md)

# Called by

- [log_mapi_connection](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/log_mapi_connection.md)
- [log_mapi_session_disconnect](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/diagnostics/log_mapi_session_disconnect.md)
- [log_mapi_transport_connection](../../../../../../../functions/crates/lpe-exchange/src/service/transport_diagnostics/log_mapi_transport_connection.md)