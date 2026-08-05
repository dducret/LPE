---
type: Rust Function
title: notification_wait_response
resource: crates/lpe-exchange/src/mapi/transport/notification_wait.rs#L31-L134
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/transport/cookies/log_session_cookie_lookup
  - functions/crates/lpe-exchange/src/mapi/transport/headers/client_flow_key
  - functions/crates/lpe-exchange/src/mapi/transport/headers/guid_counter_debug
  - functions/crates/lpe-exchange/src/mapi/transport/cookies/request_cookie
  - functions/crates/lpe-exchange/src/mapi/transport/mapi_diagnostic_response
  - functions/crates/lpe-exchange/src/mapi/transport/notification_wait/acquire_notification_wait_active_session_request
  - functions/crates/lpe-exchange/src/mapi/transport/notification_wait/notification_wait_empty_response
  - functions/crates/lpe-exchange/src/mapi/session/lifecycle/remove_session
  - functions/crates/lpe-exchange/src/mapi/session/lifecycle/session_matches
  - functions/crates/lpe-exchange/src/mapi/session/lifecycle/store_session
  - functions/crates/lpe-exchange/src/mapi/transport/notification_wait/run_notification_wait
  - functions/crates/lpe-exchange/src/mapi/transport/notification_wait/notification_wait_streaming_response
  called_by:
  - functions/crates/lpe-exchange/src/mapi/transport/handle_mapi
---

# Signature

`pub(in crate::mapi) async fn notification_wait_response<S>( store: S, endpoint: MapiEndpoint, principal: &AccountPrincipal, headers: &HeaderMap, request_id: &str, ) -> Response where S: ExchangeStore + Send + Sync + 'static,`

# Calls

- [log_session_cookie_lookup](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/cookies/log_session_cookie_lookup.md)
- [client_flow_key](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/headers/client_flow_key.md)
- [guid_counter_debug](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/headers/guid_counter_debug.md)
- [request_cookie](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/cookies/request_cookie.md)
- [mapi_diagnostic_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/mapi_diagnostic_response.md)
- [acquire_notification_wait_active_session_request](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/notification_wait/acquire_notification_wait_active_session_request.md)
- [notification_wait_empty_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/notification_wait/notification_wait_empty_response.md)
- [remove_session](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/lifecycle/remove_session.md)
- [session_matches](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/lifecycle/session_matches.md)
- [store_session](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/lifecycle/store_session.md)
- [run_notification_wait](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/notification_wait/run_notification_wait.md)
- [notification_wait_streaming_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/notification_wait/notification_wait_streaming_response.md)

# Called by

- [handle_mapi](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/handle_mapi.md)