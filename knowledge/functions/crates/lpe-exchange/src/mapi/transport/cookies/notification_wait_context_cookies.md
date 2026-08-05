---
type: Rust Function
title: notification_wait_context_cookies
resource: crates/lpe-exchange/src/mapi/transport/cookies.rs#L245-L253
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/transport/notification_wait/decorate_notification_wait_response
  - functions/crates/lpe-exchange/src/mapi/transport/notification_wait/notification_wait_empty_response
---

# Signature

`pub(in crate::mapi) fn notification_wait_context_cookies( endpoint: MapiEndpoint, session_id: &str, ) -> Vec<String>`

# Called by

- [decorate_notification_wait_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/notification_wait/decorate_notification_wait_response.md)
- [notification_wait_empty_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/notification_wait/notification_wait_empty_response.md)