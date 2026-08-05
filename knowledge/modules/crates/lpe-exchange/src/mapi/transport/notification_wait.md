---
type: Rust Module
title: notification_wait
resource: crates/lpe-exchange/src/mapi/transport/notification_wait.rs#L1-L453
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/super
  - external/axum-body-body-bytes-http-header-content-type-set-cookie-transfer-encoding-headermap-headervalue-statuscode-response-response
  - external/std-io-time-duration
  - external/tokio-stream-wrappers-receiverstream
  member_of:
  - packages/crates/lpe-exchange
---

# Contains

- [DeferredNotificationWaitTrace](../../../../../../classes/crates/lpe-exchange/src/mapi/transport/notification_wait/DeferredNotificationWaitTrace.md)
- [notification_wait_response](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/notification_wait/notification_wait_response.md)
- [run_notification_wait](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/notification_wait/run_notification_wait.md)
- [notification_wait_event_pending](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/notification_wait/notification_wait_event_pending.md)
- [notification_wait_sleep_duration](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/notification_wait/notification_wait_sleep_duration.md)
- [complete_notification_wait](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/notification_wait/complete_notification_wait.md)
- [notification_wait_final_frame](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/notification_wait/notification_wait_final_frame.md)
- [notification_wait_streaming_response](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/notification_wait/notification_wait_streaming_response.md)
- [notification_wait_trace_response](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/notification_wait/notification_wait_trace_response.md)
- [decorate_notification_wait_response](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/notification_wait/decorate_notification_wait_response.md)
- [notification_wait_empty_response](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/notification_wait/notification_wait_empty_response.md)
- [acquire_notification_wait_active_session_request](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/notification_wait/acquire_notification_wait_active_session_request.md)
- [session_id_prefix](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/notification_wait/session_id_prefix.md)

# Imports

- `super::*`
- `axum::{
    body::{Body, Bytes},
    http::{
        header::{CONTENT_TYPE, SET_COOKIE, TRANSFER_ENCODING},
        HeaderMap, HeaderValue, StatusCode,
    },
    response::Response,
}`
- `std::{io, time::Duration}`
- `tokio_stream::wrappers::ReceiverStream`

# Member of

- [lpe-exchange](../../../../../../packages/crates/lpe-exchange.md)