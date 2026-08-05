---
type: Rust Module
title: cookies
resource: crates/lpe-exchange/src/mapi/transport/cookies.rs#L1-L377
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/axum-http-header-set-cookie-headermap-headervalue-response-response
  - external/uuid-uuid
  - external/super-get-session-mapi-payload-fingerprint-rotate-session-request-sequence-token-accountprincipal-mapiendpoint-emsmdb-cookie-emsmdb-cookie-path-emsmdb-sequence-cookie-mapi-session-max-age-seconds-nspi-cookie-nspi-cookie-path-nspi-sequence-cookie
  member_of:
  - packages/crates/lpe-exchange
---

# Contains

- [request_cookie](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/cookies/request_cookie.md)
- [request_sequence_cookie](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/cookies/request_sequence_cookie.md)
- [request_sequence_cookie_matches](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/cookies/request_sequence_cookie_matches.md)
- [request_named_cookie](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/cookies/request_named_cookie.md)
- [request_named_cookie_candidates](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/cookies/request_named_cookie_candidates.md)
- [CookieValueDebug](../../../../../../classes/crates/lpe-exchange/src/mapi/transport/cookies/CookieValueDebug.md)
- [SessionCookieLookupDebug](../../../../../../classes/crates/lpe-exchange/src/mapi/transport/cookies/SessionCookieLookupDebug.md)
- [RequestCookieTransportDebug](../../../../../../classes/crates/lpe-exchange/src/mapi/transport/cookies/RequestCookieTransportDebug.md)
- [request_cookie_transport_debug](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/cookies/request_cookie_transport_debug.md)
- [session_cookie_lookup_debug](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/cookies/session_cookie_lookup_debug.md)
- [cookie_value_debug](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/cookies/cookie_value_debug.md)
- [cookie_value_suffix](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/cookies/cookie_value_suffix.md)
- [log_session_cookie_lookup](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/cookies/log_session_cookie_lookup.md)
- [session_cookie](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/cookies/session_cookie.md)
- [sequence_cookie](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/cookies/sequence_cookie.md)
- [session_context_cookies](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/cookies/session_context_cookies.md)
- [notification_wait_context_cookies](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/cookies/notification_wait_context_cookies.md)
- [routing_cookie](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/cookies/routing_cookie.md)
- [backend_cookie](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/cookies/backend_cookie.md)
- [exchange_topology_cookie](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/cookies/exchange_topology_cookie.md)
- [refresh_accepted_session_response_cookies](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/cookies/refresh_accepted_session_response_cookies.md)
- [context_cookie](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/cookies/context_cookie.md)
- [cookie_name](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/cookies/cookie_name.md)
- [sequence_cookie_name](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/cookies/sequence_cookie_name.md)
- [cookie_path](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/cookies/cookie_path.md)

# Imports

- `axum::{
    http::{header::SET_COOKIE, HeaderMap, HeaderValue},
    response::Response,
}`
- `uuid::Uuid`
- `super::{
    get_session, mapi_payload_fingerprint, rotate_session_request_sequence_token, AccountPrincipal,
    MapiEndpoint, EMSMDB_COOKIE, EMSMDB_COOKIE_PATH, EMSMDB_SEQUENCE_COOKIE,
    MAPI_SESSION_MAX_AGE_SECONDS, NSPI_COOKIE, NSPI_COOKIE_PATH, NSPI_SEQUENCE_COOKIE,
}`

# Member of

- [lpe-exchange](../../../../../../packages/crates/lpe-exchange.md)