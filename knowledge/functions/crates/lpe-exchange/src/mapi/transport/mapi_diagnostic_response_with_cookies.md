---
type: Rust Function
title: mapi_diagnostic_response_with_cookies
resource: crates/lpe-exchange/src/mapi/transport.rs#L653-L671
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/transport/mapi_response_with_cookies
  called_by:
  - functions/crates/lpe-exchange/src/mapi/session/lifecycle/reconnect_session
  - functions/crates/lpe-exchange/src/mapi/session/lifecycle/established_session_request
  - functions/crates/lpe-exchange/src/mapi/transport/disconnect_response
  - functions/crates/lpe-exchange/src/mapi/transport/ping_response
  - functions/crates/lpe-exchange/src/mapi/transport/mapi_diagnostic_response
  - functions/crates/lpe-exchange/src/mapi/transport/execute_transport_failure_response
---

# Signature

`pub(in crate::mapi) fn mapi_diagnostic_response_with_cookies( request_type: &str, request_id: &str, response_code: u16, message: &str, cookies: Vec<String>, ) -> Response`

# Calls

- [mapi_response_with_cookies](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/mapi_response_with_cookies.md)

# Called by

- [reconnect_session](../../../../../../functions/crates/lpe-exchange/src/mapi/session/lifecycle/reconnect_session.md)
- [established_session_request](../../../../../../functions/crates/lpe-exchange/src/mapi/session/lifecycle/established_session_request.md)
- [disconnect_response](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/disconnect_response.md)
- [ping_response](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/ping_response.md)
- [mapi_diagnostic_response](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/mapi_diagnostic_response.md)
- [execute_transport_failure_response](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/execute_transport_failure_response.md)