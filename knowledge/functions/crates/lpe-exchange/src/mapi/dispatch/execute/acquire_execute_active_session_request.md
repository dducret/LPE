---
type: Rust Function
title: acquire_execute_active_session_request
resource: crates/lpe-exchange/src/mapi/dispatch/execute.rs#L28-L43
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/session/lifecycle/begin_active_session_request
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_active_session_acquire_waits_for_short_outlook_overlap
---

# Signature

`pub(super) async fn acquire_execute_active_session_request( session_id: &str, ) -> Option<ActiveSessionRequest>`

# Calls

- [begin_active_session_request](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/lifecycle/begin_active_session_request.md)

# Called by

- [execute_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute_response.md)
- [execute_active_session_acquire_waits_for_short_outlook_overlap](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_active_session_acquire_waits_for_short_outlook_overlap.md)