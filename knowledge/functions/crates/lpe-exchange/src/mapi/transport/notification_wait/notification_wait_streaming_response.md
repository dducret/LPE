---
type: Rust Function
title: notification_wait_streaming_response
resource: crates/lpe-exchange/src/mapi/transport/notification_wait.rs#L343-L359
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/transport/notification_wait/decorate_notification_wait_response
  called_by:
  - functions/crates/lpe-exchange/src/mapi/transport/notification_wait/notification_wait_response
  - functions/crates/lpe-exchange/src/mapi/transport/tests/notification_wait_streaming_response_matches_exchange_completion_cookies
---

# Signature

`pub(super) fn notification_wait_streaming_response( endpoint: MapiEndpoint, request_id: &str, session_id: &str, receiver: tokio::sync::mpsc::Receiver<NotificationWaitFrame>, ) -> Response`

# Calls

- [decorate_notification_wait_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/notification_wait/decorate_notification_wait_response.md)

# Called by

- [notification_wait_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/notification_wait/notification_wait_response.md)
- [notification_wait_streaming_response_matches_exchange_completion_cookies](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/tests/notification_wait_streaming_response_matches_exchange_completion_cookies.md)