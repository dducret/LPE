---
type: Rust Function
title: request_cookie
resource: crates/lpe-exchange/src/mapi/transport/cookies.rs#L18-L23
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/transport/cookies/request_named_cookie
  - functions/crates/lpe-exchange/src/mapi/transport/cookies/cookie_name
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute_response
  - functions/crates/lpe-exchange/src/mapi/session/lifecycle/reconnect_session
  - functions/crates/lpe-exchange/src/mapi/session/lifecycle/established_session_request
  - functions/crates/lpe-exchange/src/mapi/transport/disconnect_response
  - functions/crates/lpe-exchange/src/mapi/transport/ping_response
  - functions/crates/lpe-exchange/src/mapi/transport/trace_mapi_connection
  - functions/crates/lpe-exchange/src/mapi/transport/cookies/refresh_accepted_session_response_cookies
  - functions/crates/lpe-exchange/src/mapi/transport/notification_wait/notification_wait_response
---

# Signature

`pub(in crate::mapi) fn request_cookie( endpoint: MapiEndpoint, headers: &HeaderMap, ) -> Option<String>`

# Calls

- [request_named_cookie](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/cookies/request_named_cookie.md)
- [cookie_name](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/cookies/cookie_name.md)

# Called by

- [execute_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute_response.md)
- [reconnect_session](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/lifecycle/reconnect_session.md)
- [established_session_request](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/lifecycle/established_session_request.md)
- [disconnect_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/disconnect_response.md)
- [ping_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/ping_response.md)
- [trace_mapi_connection](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/trace_mapi_connection.md)
- [refresh_accepted_session_response_cookies](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/cookies/refresh_accepted_session_response_cookies.md)
- [notification_wait_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/notification_wait/notification_wait_response.md)