---
type: Rust Function
title: append_notification_data
resource: crates/lpe-exchange/src/mapi/notifications.rs#L762-L882
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/write_u16
  - functions/crates/lpe-exchange/src/mapi/notifications/append_event_object_ids
  - functions/crates/lpe-exchange/src/mapi/notifications/append_wire_id
  - functions/crates/lpe-exchange/src/mapi/notifications/event_object_id
  - functions/crates/lpe-core/src/sieve/Parser/expect
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/mapi/notifications/rop_notify_response
---

# Signature

`fn append_notification_data( response: &mut Vec<u8>, identity_codec: &crate::mapi::identity::MapiIdentityCodec, event: &MapiNotificationEvent, )`

# Calls

- [write_u16](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/write_u16.md)
- [append_event_object_ids](../../../../../../functions/crates/lpe-exchange/src/mapi/notifications/append_event_object_ids.md)
- [append_wire_id](../../../../../../functions/crates/lpe-exchange/src/mapi/notifications/append_wire_id.md)
- [event_object_id](../../../../../../functions/crates/lpe-exchange/src/mapi/notifications/event_object_id.md)
- [expect](../../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)
- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [rop_notify_response](../../../../../../functions/crates/lpe-exchange/src/mapi/notifications/rop_notify_response.md)