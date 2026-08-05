---
type: Rust Function
title: notification_wait_response
resource: crates/lpe-exchange/src/mapi/transport/notification_wait.rs#L31-L105
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/transport/cookies/log_session_cookie_lookup
  - functions/crates/lpe-exchange/src/mapi/transport/cookies/request_cookie
  - functions/crates/lpe-exchange/src/mapi/transport/mapi_diagnostic_response
  - functions/crates/lpe-exchange/src/mapi/session/lifecycle/get_session
  - functions/crates/lpe-exchange/src/mapi/session/lifecycle/session_matches
  - functions/crates/lpe-exchange/src/mapi/transport/notification_wait/notification_wait_empty_response
  - functions/crates/lpe-exchange/src/mapi/transport/notification_wait/run_notification_wait
  - functions/crates/lpe-exchange/src/mapi/transport/notification_wait/notification_wait_streaming_response
  called_by:
  - functions/crates/lpe-exchange/src/mapi/transport/handle_mapi
---

# Signature

`pub(in crate::mapi) async fn notification_wait_response<S>( store: S, endpoint: MapiEndpoint, principal: &AccountPrincipal, headers: &HeaderMap, request_id: &str, ) -> Response where S: ExchangeStore + Send + Sync + 'static,`

# Calls

- [log_session_cookie_lookup](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/cookies/log_session_cookie_lookup.md)
- [request_cookie](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/cookies/request_cookie.md)
- [mapi_diagnostic_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/mapi_diagnostic_response.md)
- [get_session](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/lifecycle/get_session.md)
- [session_matches](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/lifecycle/session_matches.md)
- [notification_wait_empty_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/notification_wait/notification_wait_empty_response.md)
- [run_notification_wait](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/notification_wait/run_notification_wait.md)
- [notification_wait_streaming_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/notification_wait/notification_wait_streaming_response.md)

# Called by

- [handle_mapi](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/handle_mapi.md)