---
type: Rust Function
title: append_preexisting_notification_responses
resource: crates/lpe-exchange/src/mapi/dispatch.rs#L1713-L1728
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/notifications/rop_notify_response
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute_rops
---

# Signature

`fn append_preexisting_notification_responses( responses: &mut Vec<u8>, identity_codec: &crate::mapi::identity::MapiIdentityCodec, deliveries: Vec<(u32, u8, MapiNotificationEvent)>, ) -> usize`

# Calls

- [rop_notify_response](../../../../../../functions/crates/lpe-exchange/src/mapi/notifications/rop_notify_response.md)

# Called by

- [execute_rops](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute_rops.md)