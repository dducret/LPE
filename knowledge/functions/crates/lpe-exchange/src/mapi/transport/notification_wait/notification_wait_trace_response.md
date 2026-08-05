---
type: Rust Function
title: notification_wait_trace_response
resource: crates/lpe-exchange/src/mapi/transport/notification_wait.rs#L361-L381
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/calendar_identity/MapiMailStoreSnapshot/empty
  - functions/crates/lpe-exchange/src/mapi/transport/notification_wait/decorate_notification_wait_response
  called_by:
  - functions/crates/lpe-exchange/src/mapi/transport/notification_wait/complete_notification_wait
---

# Signature

`fn notification_wait_trace_response( endpoint: MapiEndpoint, request_id: &str, session_id: &str, response_code: u16, body: Vec<u8>, ) -> Response`

# Calls

- [empty](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/calendar_identity/MapiMailStoreSnapshot/empty.md)
- [decorate_notification_wait_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/notification_wait/decorate_notification_wait_response.md)

# Called by

- [complete_notification_wait](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/notification_wait/complete_notification_wait.md)