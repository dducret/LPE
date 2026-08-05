---
type: Rust Function
title: rop_register_notification_response
resource: crates/lpe-exchange/src/mapi/notifications.rs#L292-L296
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/notification_subscriptions/append_register_notification_response
  - functions/crates/lpe-exchange/src/mapi/notifications/register_notification_success_response_matches_microsoft_wire_shape
---

# Signature

`pub(in crate::mapi) fn rop_register_notification_response(request: &RopRequest) -> Vec<u8>`

# Called by

- [append_register_notification_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/notification_subscriptions/append_register_notification_response.md)
- [register_notification_success_response_matches_microsoft_wire_shape](../../../../../../functions/crates/lpe-exchange/src/mapi/notifications/register_notification_success_response_matches_microsoft_wire_shape.md)