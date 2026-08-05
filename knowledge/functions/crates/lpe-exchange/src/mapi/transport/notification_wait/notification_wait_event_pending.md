---
type: Rust Function
title: notification_wait_event_pending
resource: crates/lpe-exchange/src/mapi/transport/notification_wait.rs#L191-L257
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/transport/notification_wait/acquire_notification_wait_active_session_request
  - functions/crates/lpe-exchange/src/mapi/session/lifecycle/remove_session
  - functions/crates/lpe-exchange/src/mapi/session/lifecycle/session_matches
  - functions/crates/lpe-exchange/src/mapi/session/table_notifications/MapiSession/pending_notification_count
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/poll_mapi_notifications
  - functions/crates/lpe-exchange/src/mapi/session/table_notifications/MapiSession/matching_notifications
  - functions/crates/lpe-exchange/src/mapi/session/table_notifications/MapiSession/record_notification
  - functions/crates/lpe-exchange/src/mapi/session/lifecycle/store_session
  called_by:
  - functions/crates/lpe-exchange/src/mapi/transport/notification_wait/run_notification_wait
---

# Signature

`async fn notification_wait_event_pending<S>( store: &S, endpoint: MapiEndpoint, principal: &AccountPrincipal, session_id: &str, ) -> std::result::Result<Option<bool>, u16> where S: ExchangeStore,`

# Calls

- [acquire_notification_wait_active_session_request](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/notification_wait/acquire_notification_wait_active_session_request.md)
- [remove_session](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/lifecycle/remove_session.md)
- [session_matches](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/lifecycle/session_matches.md)
- [pending_notification_count](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/table_notifications/MapiSession/pending_notification_count.md)
- [poll_mapi_notifications](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/poll_mapi_notifications.md)
- [matching_notifications](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/table_notifications/MapiSession/matching_notifications.md)
- [record_notification](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/table_notifications/MapiSession/record_notification.md)
- [store_session](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/lifecycle/store_session.md)

# Called by

- [run_notification_wait](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/notification_wait/run_notification_wait.md)