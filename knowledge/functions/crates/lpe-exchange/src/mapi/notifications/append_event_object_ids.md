---
type: Rust Function
title: append_event_object_ids
resource: crates/lpe-exchange/src/mapi/notifications.rs#L840-L854
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/notifications/append_wire_id
  - functions/crates/lpe-exchange/src/mapi/notifications/event_object_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi/notifications/append_notification_data
---

# Signature

`fn append_event_object_ids( response: &mut Vec<u8>, identity_codec: &crate::mapi::identity::MapiIdentityCodec, event: &MapiNotificationEvent, message_event: bool, )`

# Calls

- [append_wire_id](../../../../../../functions/crates/lpe-exchange/src/mapi/notifications/append_wire_id.md)
- [event_object_id](../../../../../../functions/crates/lpe-exchange/src/mapi/notifications/event_object_id.md)

# Called by

- [append_notification_data](../../../../../../functions/crates/lpe-exchange/src/mapi/notifications/append_notification_data.md)