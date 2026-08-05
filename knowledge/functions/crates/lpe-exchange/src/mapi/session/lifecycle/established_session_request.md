---
type: Rust Function
title: established_session_request
resource: crates/lpe-exchange/src/mapi/session/lifecycle.rs#L357-L401
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/transport/cookies/request_cookie
  - functions/crates/lpe-exchange/src/mapi/transport/mapi_diagnostic_response
  - functions/crates/lpe-exchange/src/mapi/session/lifecycle/begin_active_session_request
  - functions/crates/lpe-exchange/src/mapi/transport/mapi_diagnostic_response_with_cookies
  - functions/crates/lpe-exchange/src/mapi/transport/cookies/session_context_cookies
  - functions/crates/lpe-exchange/src/mapi/session/lifecycle/get_session
  - functions/crates/lpe-exchange/src/mapi/session/lifecycle/session_matches
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_transport_request
  - functions/crates/lpe-exchange/src/mapi/session/lifecycle/store_session
  called_by:
  - functions/crates/lpe-exchange/src/mapi/transport/handle_mapi
---

# Signature

`pub(in crate::mapi) fn established_session_request( endpoint: MapiEndpoint, principal: &AccountPrincipal, headers: &HeaderMap, request_type: &str, request_id: &str, ) -> std::result::Result<ActiveSessionRequest, Response>`

# Calls

- [request_cookie](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/cookies/request_cookie.md)
- [mapi_diagnostic_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/mapi_diagnostic_response.md)
- [begin_active_session_request](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/lifecycle/begin_active_session_request.md)
- [mapi_diagnostic_response_with_cookies](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/mapi_diagnostic_response_with_cookies.md)
- [session_context_cookies](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/cookies/session_context_cookies.md)
- [get_session](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/lifecycle/get_session.md)
- [session_matches](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/lifecycle/session_matches.md)
- [record_transport_request](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_transport_request.md)
- [store_session](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/lifecycle/store_session.md)

# Called by

- [handle_mapi](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/handle_mapi.md)