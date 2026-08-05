---
type: Rust Function
title: notification_wait_empty_response
resource: crates/lpe-exchange/src/mapi/transport/notification_wait.rs#L395-L409
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/transport/mapi_response_with_cookies
  - functions/crates/lpe-exchange/src/mapi/notifications/notification_wait_body
  - functions/crates/lpe-exchange/src/mapi/transport/cookies/notification_wait_context_cookies
  - functions/crates/lpe-exchange/src/mapi/transport/insert_header
  called_by:
  - functions/crates/lpe-exchange/src/mapi/transport/notification_wait/notification_wait_response
  - functions/crates/lpe-exchange/src/mapi/transport/tests/notification_wait_empty_response_reports_success_with_empty_body
---

# Signature

`pub(in crate::mapi) fn notification_wait_empty_response( endpoint: MapiEndpoint, request_id: &str, session_id: &str, ) -> Response`

# Calls

- [mapi_response_with_cookies](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/mapi_response_with_cookies.md)
- [notification_wait_body](../../../../../../../functions/crates/lpe-exchange/src/mapi/notifications/notification_wait_body.md)
- [notification_wait_context_cookies](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/cookies/notification_wait_context_cookies.md)
- [insert_header](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/insert_header.md)

# Called by

- [notification_wait_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/notification_wait/notification_wait_response.md)
- [notification_wait_empty_response_reports_success_with_empty_body](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/tests/notification_wait_empty_response_reports_success_with_empty_body.md)