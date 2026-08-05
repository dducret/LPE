---
type: Rust Function
title: ping_response
resource: crates/lpe-exchange/src/mapi/transport.rs#L591-L636
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/transport/headers/content_length_header
  - functions/crates/lpe-exchange/src/mapi/transport/mapi_diagnostic_response
  - functions/crates/lpe-exchange/src/mapi/transport/cookies/log_session_cookie_lookup
  - functions/crates/lpe-exchange/src/mapi/transport/cookies/request_cookie
  - functions/crates/lpe-exchange/src/mapi/session/lifecycle/begin_active_session_request
  - functions/crates/lpe-exchange/src/mapi/transport/mapi_diagnostic_response_with_cookies
  - functions/crates/lpe-exchange/src/mapi/transport/cookies/session_context_cookies
  - functions/crates/lpe-exchange/src/mapi/session/lifecycle/remove_session
  - functions/crates/lpe-exchange/src/mapi/session/lifecycle/session_matches
  - functions/crates/lpe-exchange/src/mapi/session/lifecycle/store_session
  - functions/crates/lpe-exchange/src/mapi/transport/mapi_response
  called_by:
  - functions/crates/lpe-exchange/src/mapi/transport/handle_mapi
  - functions/crates/lpe-exchange/src/mapi/transport/tests/ping_accepts_missing_or_prior_mapi_sequence_cookie
  - functions/crates/lpe-exchange/src/mapi/transport/tests/active_session_ping_failure_returns_current_session_cookies
---

# Signature

`pub(in crate::mapi) fn ping_response( endpoint: MapiEndpoint, principal: &AccountPrincipal, headers: &HeaderMap, body: &[u8], request_id: &str, ) -> Response`

# Calls

- [content_length_header](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/headers/content_length_header.md)
- [mapi_diagnostic_response](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/mapi_diagnostic_response.md)
- [log_session_cookie_lookup](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/cookies/log_session_cookie_lookup.md)
- [request_cookie](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/cookies/request_cookie.md)
- [begin_active_session_request](../../../../../../functions/crates/lpe-exchange/src/mapi/session/lifecycle/begin_active_session_request.md)
- [mapi_diagnostic_response_with_cookies](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/mapi_diagnostic_response_with_cookies.md)
- [session_context_cookies](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/cookies/session_context_cookies.md)
- [remove_session](../../../../../../functions/crates/lpe-exchange/src/mapi/session/lifecycle/remove_session.md)
- [session_matches](../../../../../../functions/crates/lpe-exchange/src/mapi/session/lifecycle/session_matches.md)
- [store_session](../../../../../../functions/crates/lpe-exchange/src/mapi/session/lifecycle/store_session.md)
- [mapi_response](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/mapi_response.md)

# Called by

- [handle_mapi](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/handle_mapi.md)
- [ping_accepts_missing_or_prior_mapi_sequence_cookie](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/tests/ping_accepts_missing_or_prior_mapi_sequence_cookie.md)
- [active_session_ping_failure_returns_current_session_cookies](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/tests/active_session_ping_failure_returns_current_session_cookies.md)