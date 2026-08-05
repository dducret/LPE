---
type: Rust Function
title: log_session_cookie_lookup
resource: crates/lpe-exchange/src/mapi/transport/cookies.rs#L168-L200
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/transport/cookies/session_cookie_lookup_debug
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute_response
  - functions/crates/lpe-exchange/src/mapi/transport/handle_mapi
  - functions/crates/lpe-exchange/src/mapi/transport/disconnect_response
  - functions/crates/lpe-exchange/src/mapi/transport/ping_response
  - functions/crates/lpe-exchange/src/mapi/transport/notification_wait/notification_wait_response
---

# Signature

`pub(in crate::mapi) fn log_session_cookie_lookup( endpoint: MapiEndpoint, principal: &AccountPrincipal, headers: &HeaderMap, request_type: &str, )`

# Calls

- [session_cookie_lookup_debug](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/cookies/session_cookie_lookup_debug.md)

# Called by

- [execute_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute_response.md)
- [handle_mapi](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/handle_mapi.md)
- [disconnect_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/disconnect_response.md)
- [ping_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/ping_response.md)
- [notification_wait_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/notification_wait/notification_wait_response.md)