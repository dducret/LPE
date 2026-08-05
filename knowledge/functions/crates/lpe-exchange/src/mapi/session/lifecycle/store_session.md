---
type: Rust Function
title: store_session
resource: crates/lpe-exchange/src/mapi/session/lifecycle.rs#L298-L306
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/session/lifecycle/sessions
  - functions/crates/lpe-exchange/src/mapi/session/lifecycle/prune_expired_sessions_locked
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute_response
  - functions/crates/lpe-exchange/src/mapi/session/lifecycle/reconnect_session
  - functions/crates/lpe-exchange/src/mapi/session/lifecycle/execute_rpc_emsmdb_rops
  - functions/crates/lpe-exchange/src/mapi/session/lifecycle/established_session_request
  - functions/crates/lpe-exchange/src/mapi/transport/ping_response
  - functions/crates/lpe-exchange/src/mapi/transport/notification_wait/notification_wait_event_pending
---

# Signature

`pub(in crate::mapi) fn store_session(session_id: String, mut session: MapiSession)`

# Calls

- [sessions](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/lifecycle/sessions.md)
- [prune_expired_sessions_locked](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/lifecycle/prune_expired_sessions_locked.md)

# Called by

- [execute_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute_response.md)
- [reconnect_session](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/lifecycle/reconnect_session.md)
- [execute_rpc_emsmdb_rops](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/lifecycle/execute_rpc_emsmdb_rops.md)
- [established_session_request](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/lifecycle/established_session_request.md)
- [ping_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/ping_response.md)
- [notification_wait_event_pending](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/notification_wait/notification_wait_event_pending.md)