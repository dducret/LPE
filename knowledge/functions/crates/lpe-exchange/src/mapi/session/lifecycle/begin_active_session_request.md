---
type: Rust Function
title: begin_active_session_request
resource: crates/lpe-exchange/src/mapi/session/lifecycle.rs#L29-L42
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/session/lifecycle/active_session_requests
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute/acquire_execute_active_session_request
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_active_session_acquire_waits_for_short_outlook_overlap
  - functions/crates/lpe-exchange/src/mapi/session/lifecycle/begin_active_session_request_for_test
  - functions/crates/lpe-exchange/src/mapi/session/lifecycle/established_session_request
  - functions/crates/lpe-exchange/src/mapi/session/tests/reconnect_session_rejects_active_context
  - functions/crates/lpe-exchange/src/mapi/transport/disconnect_response
  - functions/crates/lpe-exchange/src/mapi/transport/ping_response
  - functions/crates/lpe-exchange/src/mapi/transport/notification_wait/acquire_notification_wait_active_session_request
  - functions/crates/lpe-exchange/src/mapi/transport/tests/active_session_ping_failure_returns_current_session_cookies
  - functions/crates/lpe-exchange/src/mapi/transport/tests/notification_wait_active_session_acquire_waits_for_short_outlook_overlap
---

# Signature

`pub(in crate::mapi) fn begin_active_session_request( session_id: &str, ) -> Option<ActiveSessionRequest>`

# Calls

- [active_session_requests](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/lifecycle/active_session_requests.md)

# Called by

- [acquire_execute_active_session_request](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute/acquire_execute_active_session_request.md)
- [execute_active_session_acquire_waits_for_short_outlook_overlap](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_active_session_acquire_waits_for_short_outlook_overlap.md)
- [begin_active_session_request_for_test](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/lifecycle/begin_active_session_request_for_test.md)
- [established_session_request](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/lifecycle/established_session_request.md)
- [reconnect_session_rejects_active_context](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/tests/reconnect_session_rejects_active_context.md)
- [disconnect_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/disconnect_response.md)
- [ping_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/ping_response.md)
- [acquire_notification_wait_active_session_request](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/notification_wait/acquire_notification_wait_active_session_request.md)
- [active_session_ping_failure_returns_current_session_cookies](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/tests/active_session_ping_failure_returns_current_session_cookies.md)
- [notification_wait_active_session_acquire_waits_for_short_outlook_overlap](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/tests/notification_wait_active_session_acquire_waits_for_short_outlook_overlap.md)