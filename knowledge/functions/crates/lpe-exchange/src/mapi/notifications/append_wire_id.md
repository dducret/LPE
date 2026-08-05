---
type: Rust Function
title: append_wire_id
resource: crates/lpe-exchange/src/mapi/notifications.rs#L824-L834
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/notifications/rop_hierarchy_table_row_modified_response
  - functions/crates/lpe-exchange/src/mapi/notifications/append_notification_data
  - functions/crates/lpe-exchange/src/mapi/notifications/append_event_object_ids
---

# Signature

`fn append_wire_id( response: &mut Vec<u8>, identity_codec: &crate::mapi::identity::MapiIdentityCodec, object_id: u64, )`

# Called by

- [rop_hierarchy_table_row_modified_response](../../../../../../functions/crates/lpe-exchange/src/mapi/notifications/rop_hierarchy_table_row_modified_response.md)
- [append_notification_data](../../../../../../functions/crates/lpe-exchange/src/mapi/notifications/append_notification_data.md)
- [append_event_object_ids](../../../../../../functions/crates/lpe-exchange/src/mapi/notifications/append_event_object_ids.md)