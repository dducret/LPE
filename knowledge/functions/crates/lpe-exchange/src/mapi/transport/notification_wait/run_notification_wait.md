---
type: Rust Function
title: run_notification_wait
resource: crates/lpe-exchange/src/mapi/transport/notification_wait.rs#L136-L208
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/transport/notification_wait/notification_wait_event_pending
  - functions/crates/lpe-exchange/src/mapi/notifications/notification_wait_body
  - functions/crates/lpe-exchange/src/mapi/transport/notification_wait/complete_notification_wait
  - functions/crates/lpe-exchange/src/mapi/transport/notification_wait/notification_wait_sleep_duration
  called_by:
  - functions/crates/lpe-exchange/src/mapi/transport/notification_wait/notification_wait_response
---

# Signature

`async fn run_notification_wait<S>( store: S, endpoint: MapiEndpoint, principal: AccountPrincipal, request_headers: HeaderMap, request_id: String, session_id: String, sender: NotificationWaitSender, ) where S: ExchangeStore + Send + Sync + 'static,`

# Calls

- [notification_wait_event_pending](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/notification_wait/notification_wait_event_pending.md)
- [notification_wait_body](../../../../../../../functions/crates/lpe-exchange/src/mapi/notifications/notification_wait_body.md)
- [complete_notification_wait](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/notification_wait/complete_notification_wait.md)
- [notification_wait_sleep_duration](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/notification_wait/notification_wait_sleep_duration.md)

# Called by

- [notification_wait_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/notification_wait/notification_wait_response.md)