---
type: Rust Function
title: session_context_cookies
resource: crates/lpe-exchange/src/mapi/transport/cookies.rs#L228-L239
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute_response
  - functions/crates/lpe-exchange/src/mapi/nspi/bind_response
  - functions/crates/lpe-exchange/src/mapi/session/lifecycle/reconnect_session
  - functions/crates/lpe-exchange/src/mapi/session/lifecycle/established_session_request
  - functions/crates/lpe-exchange/src/mapi/transport/connect_response
  - functions/crates/lpe-exchange/src/mapi/transport/disconnect_response
  - functions/crates/lpe-exchange/src/mapi/transport/disconnect_success_response
  - functions/crates/lpe-exchange/src/mapi/transport/ping_response
  - functions/crates/lpe-exchange/src/mapi/transport/cookies/refresh_accepted_session_response_cookies
  - functions/crates/lpe-exchange/src/mapi/transport/tests/regular_mapi_responses_include_exchange_routing_cookies
  - functions/crates/lpe-exchange/src/mapi/transport/tests/accepted_response_rotates_the_mapi_sequence_cookie
---

# Signature

`pub(in crate::mapi) fn session_context_cookies( endpoint: MapiEndpoint, session_id: &str, expired: bool, ) -> Vec<String>`

# Called by

- [execute_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute_response.md)
- [bind_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/bind_response.md)
- [reconnect_session](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/lifecycle/reconnect_session.md)
- [established_session_request](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/lifecycle/established_session_request.md)
- [connect_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/connect_response.md)
- [disconnect_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/disconnect_response.md)
- [disconnect_success_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/disconnect_success_response.md)
- [ping_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/ping_response.md)
- [refresh_accepted_session_response_cookies](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/cookies/refresh_accepted_session_response_cookies.md)
- [regular_mapi_responses_include_exchange_routing_cookies](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/tests/regular_mapi_responses_include_exchange_routing_cookies.md)
- [accepted_response_rotates_the_mapi_sequence_cookie](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/tests/accepted_response_rotates_the_mapi_sequence_cookie.md)