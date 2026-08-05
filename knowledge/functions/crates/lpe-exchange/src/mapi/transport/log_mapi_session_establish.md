---
type: Rust Function
title: log_mapi_session_establish
resource: crates/lpe-exchange/src/mapi/transport.rs#L426-L488
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/transport/cookies/cookie_value_debug
  - functions/crates/lpe-exchange/src/mapi/outlook_startup/configured_smart_input_variant
  called_by:
  - functions/crates/lpe-exchange/src/mapi/nspi/bind_response
  - functions/crates/lpe-exchange/src/mapi/transport/connect_response
---

# Signature

`pub(in crate::mapi) fn log_mapi_session_establish( endpoint: MapiEndpoint, principal: &AccountPrincipal, headers: &HeaderMap, request_type: &str, request_id: &str, session_id: &str, reconnected: bool, )`

# Calls

- [cookie_value_debug](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/cookies/cookie_value_debug.md)
- [configured_smart_input_variant](../../../../../../functions/crates/lpe-exchange/src/mapi/outlook_startup/configured_smart_input_variant.md)

# Called by

- [bind_response](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/bind_response.md)
- [connect_response](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/connect_response.md)