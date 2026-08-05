---
type: Rust Function
title: bind_response
resource: crates/lpe-exchange/src/mapi/nspi.rs#L132-L163
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/session/lifecycle/reconnect_session
  - functions/crates/lpe-exchange/src/mapi/session/lifecycle/create_session
  - functions/crates/lpe-exchange/src/mapi/transport/log_mapi_session_establish
  - functions/crates/lpe-exchange/src/mapi/transport/cookies/session_context_cookies
  - functions/crates/lpe-exchange/src/mapi/transport/mapi_response_with_cookies
  called_by:
  - functions/crates/lpe-exchange/src/mapi/nspi/handle_nspi_request
---

# Signature

`pub(in crate::mapi) fn bind_response( endpoint: MapiEndpoint, principal: &AccountPrincipal, headers: &HeaderMap, request_id: &str, ) -> Response`

# Calls

- [reconnect_session](../../../../../../functions/crates/lpe-exchange/src/mapi/session/lifecycle/reconnect_session.md)
- [create_session](../../../../../../functions/crates/lpe-exchange/src/mapi/session/lifecycle/create_session.md)
- [log_mapi_session_establish](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/log_mapi_session_establish.md)
- [session_context_cookies](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/cookies/session_context_cookies.md)
- [mapi_response_with_cookies](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/mapi_response_with_cookies.md)

# Called by

- [handle_nspi_request](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/handle_nspi_request.md)