---
type: Rust Function
title: disconnect_response
resource: crates/lpe-exchange/src/mapi/transport.rs#L510-L571
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/transport/cookies/log_session_cookie_lookup
  - functions/crates/lpe-exchange/src/mapi/transport/cookies/request_cookie
  - functions/crates/lpe-exchange/src/mapi/transport/mapi_diagnostic_response
  - functions/crates/lpe-exchange/src/mapi/session/lifecycle/begin_active_session_request
  - functions/crates/lpe-exchange/src/mapi/transport/mapi_diagnostic_response_with_cookies
  - functions/crates/lpe-exchange/src/mapi/transport/cookies/session_context_cookies
  - functions/crates/lpe-exchange/src/mapi/session/lifecycle/remove_session
  - functions/crates/lpe-exchange/src/mapi/transport/disconnect_success_response
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_transport_request
  - functions/crates/lpe-exchange/src/mapi/transport/diagnostics/log_mapi_session_disconnect
  called_by:
  - functions/crates/lpe-exchange/src/mapi/nspi/handle_nspi_request
  - functions/crates/lpe-exchange/src/mapi/transport/handle_mapi
---

# Signature

`pub(in crate::mapi) fn disconnect_response( endpoint: MapiEndpoint, principal: &AccountPrincipal, headers: &HeaderMap, request_id: &str, response_request_type: &str, ) -> Response`

# Calls

- [log_session_cookie_lookup](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/cookies/log_session_cookie_lookup.md)
- [request_cookie](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/cookies/request_cookie.md)
- [mapi_diagnostic_response](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/mapi_diagnostic_response.md)
- [begin_active_session_request](../../../../../../functions/crates/lpe-exchange/src/mapi/session/lifecycle/begin_active_session_request.md)
- [mapi_diagnostic_response_with_cookies](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/mapi_diagnostic_response_with_cookies.md)
- [session_context_cookies](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/cookies/session_context_cookies.md)
- [remove_session](../../../../../../functions/crates/lpe-exchange/src/mapi/session/lifecycle/remove_session.md)
- [disconnect_success_response](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/disconnect_success_response.md)
- [record_transport_request](../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_transport_request.md)
- [log_mapi_session_disconnect](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/diagnostics/log_mapi_session_disconnect.md)

# Called by

- [handle_nspi_request](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/handle_nspi_request.md)
- [handle_mapi](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/handle_mapi.md)