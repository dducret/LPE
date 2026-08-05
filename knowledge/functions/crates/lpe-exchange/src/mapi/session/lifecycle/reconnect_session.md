---
type: Rust Function
title: reconnect_session
resource: crates/lpe-exchange/src/mapi/session/lifecycle.rs#L56-L100
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/transport/cookies/request_cookie
  - functions/crates/lpe-exchange/src/mapi/session/lifecycle/session_request_is_active
  - functions/crates/lpe-exchange/src/mapi/transport/mapi_diagnostic_response_with_cookies
  - functions/crates/lpe-exchange/src/mapi/transport/cookies/session_context_cookies
  - functions/crates/lpe-exchange/src/mapi/session/lifecycle/remove_session
  - functions/crates/lpe-exchange/src/mapi/transport/mapi_diagnostic_response
  - functions/crates/lpe-exchange/src/mapi/session/lifecycle/session_matches
  - functions/crates/lpe-exchange/src/mapi/session/lifecycle/store_session
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_transport_request
  called_by:
  - functions/crates/lpe-exchange/src/mapi/nspi/bind_response
  - functions/crates/lpe-exchange/src/mapi/session/tests/reconnect_session_rejects_active_context
  - functions/crates/lpe-exchange/src/mapi/session/tests/reconnect_session_replaces_the_prior_emsmdb_context
  - functions/crates/lpe-exchange/src/mapi/transport/connect_response
---

# Signature

`pub(in crate::mapi) fn reconnect_session( endpoint: MapiEndpoint, principal: &AccountPrincipal, headers: &HeaderMap, request_type: &str, request_id: &str, ) -> std::result::Result<Option<String>, Response>`

# Calls

- [request_cookie](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/cookies/request_cookie.md)
- [session_request_is_active](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/lifecycle/session_request_is_active.md)
- [mapi_diagnostic_response_with_cookies](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/mapi_diagnostic_response_with_cookies.md)
- [session_context_cookies](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/cookies/session_context_cookies.md)
- [remove_session](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/lifecycle/remove_session.md)
- [mapi_diagnostic_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/mapi_diagnostic_response.md)
- [session_matches](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/lifecycle/session_matches.md)
- [store_session](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/lifecycle/store_session.md)
- [record_transport_request](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_transport_request.md)

# Called by

- [bind_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/bind_response.md)
- [reconnect_session_rejects_active_context](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/tests/reconnect_session_rejects_active_context.md)
- [reconnect_session_replaces_the_prior_emsmdb_context](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/tests/reconnect_session_replaces_the_prior_emsmdb_context.md)
- [connect_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/connect_response.md)