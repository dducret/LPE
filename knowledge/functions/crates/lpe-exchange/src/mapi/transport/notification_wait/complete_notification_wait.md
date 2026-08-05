---
type: Rust Function
title: complete_notification_wait
resource: crates/lpe-exchange/src/mapi/transport/notification_wait.rs#L270-L300
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/transport/notification_wait/notification_wait_final_frame
  - functions/crates/lpe-exchange/src/mapi/transport/finalize_mapi_response
  - functions/crates/lpe-exchange/src/mapi/transport/notification_wait/notification_wait_trace_response
  - functions/crates/lpe-exchange/src/mapi/transport/log_mapi_connection
  called_by:
  - functions/crates/lpe-exchange/src/mapi/transport/notification_wait/run_notification_wait
---

# Signature

`async fn complete_notification_wait( endpoint: MapiEndpoint, principal: &AccountPrincipal, request_headers: &HeaderMap, request_id: &str, session_id: &str, response_code: u16, body: Vec<u8>, started_at: std::time::Instant, start_time: std::time::SystemTime, sender: NotificationWaitSender, )`

# Calls

- [notification_wait_final_frame](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/notification_wait/notification_wait_final_frame.md)
- [finalize_mapi_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/finalize_mapi_response.md)
- [notification_wait_trace_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/notification_wait/notification_wait_trace_response.md)
- [log_mapi_connection](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/log_mapi_connection.md)

# Called by

- [run_notification_wait](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/notification_wait/run_notification_wait.md)