---
type: Rust Function
title: decorate_notification_wait_response
resource: crates/lpe-exchange/src/mapi/transport/notification_wait.rs#L364-L393
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/transport/insert_header
  - functions/crates/lpe-exchange/src/mapi/transport/cookies/notification_wait_context_cookies
  - functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str
  called_by:
  - functions/crates/lpe-exchange/src/mapi/transport/notification_wait/notification_wait_streaming_response
  - functions/crates/lpe-exchange/src/mapi/transport/notification_wait/notification_wait_trace_response
---

# Signature

`fn decorate_notification_wait_response( response: &mut Response, endpoint: MapiEndpoint, request_id: &str, response_code: u16, session_id: &str, )`

# Calls

- [insert_header](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/insert_header.md)
- [notification_wait_context_cookies](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/cookies/notification_wait_context_cookies.md)
- [from_str](../../../../../../../functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str.md)

# Called by

- [notification_wait_streaming_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/notification_wait/notification_wait_streaming_response.md)
- [notification_wait_trace_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/notification_wait/notification_wait_trace_response.md)