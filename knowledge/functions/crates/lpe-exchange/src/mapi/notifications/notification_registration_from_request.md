---
type: Rust Function
title: notification_registration_from_request
resource: crates/lpe-exchange/src/mapi/notifications.rs#L905-L920
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/notification_types
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/notification_want_whole_store
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/notification_folder_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/notification_subscriptions/append_register_notification_response
  - functions/crates/lpe-exchange/src/mapi/session/tests/notification_subscription_preserves_rop_logon_id_through_rop_notify
---

# Signature

`pub(in crate::mapi) fn notification_registration_from_request( request: &RopRequest, logon_id: u8, ) -> MapiNotificationRegistration`

# Calls

- [notification_types](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/notification_types.md)
- [notification_want_whole_store](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/notification_want_whole_store.md)
- [notification_folder_id](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/notification_folder_id.md)

# Called by

- [append_register_notification_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/notification_subscriptions/append_register_notification_response.md)
- [notification_subscription_preserves_rop_logon_id_through_rop_notify](../../../../../../functions/crates/lpe-exchange/src/mapi/session/tests/notification_subscription_preserves_rop_logon_id_through_rop_notify.md)