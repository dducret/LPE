---
type: Rust Method
title: record_last_inbox_notification_registration_context
resource: crates/lpe-exchange/src/mapi/session.rs#L432-L438
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/notification_subscriptions/append_register_notification_response
---

# Signature

`pub(in crate::mapi) fn record_last_inbox_notification_registration_context( &mut self, context: String, )`

# Called by

- [append_register_notification_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/notification_subscriptions/append_register_notification_response.md)