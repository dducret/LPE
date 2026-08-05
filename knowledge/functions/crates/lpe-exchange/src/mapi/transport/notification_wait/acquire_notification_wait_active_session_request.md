---
type: Rust Function
title: acquire_notification_wait_active_session_request
resource: crates/lpe-exchange/src/mapi/transport/notification_wait.rs#L430-L445
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/session/lifecycle/begin_active_session_request
  called_by:
  - functions/crates/lpe-exchange/src/mapi/transport/notification_wait/notification_wait_response
  - functions/crates/lpe-exchange/src/mapi/transport/notification_wait/notification_wait_event_pending
  - functions/crates/lpe-exchange/src/mapi/transport/tests/notification_wait_active_session_acquire_waits_for_short_outlook_overlap
---

# Signature

`pub(in crate::mapi) async fn acquire_notification_wait_active_session_request( session_id: &str, ) -> Option<ActiveSessionRequest>`

# Calls

- [begin_active_session_request](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/lifecycle/begin_active_session_request.md)

# Called by

- [notification_wait_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/notification_wait/notification_wait_response.md)
- [notification_wait_event_pending](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/notification_wait/notification_wait_event_pending.md)
- [notification_wait_active_session_acquire_waits_for_short_outlook_overlap](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/tests/notification_wait_active_session_acquire_waits_for_short_outlook_overlap.md)