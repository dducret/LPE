---
type: Rust Function
title: rop_hierarchy_table_row_modified_response
resource: crates/lpe-exchange/src/mapi/notifications.rs#L729-L760
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/protocol/WbxmlCodePage/tryfrom-u8/try_from
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/write_u16
  - functions/crates/lpe-exchange/src/mapi/notifications/append_wire_id
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/write_u64
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute_rops
  - functions/crates/lpe-exchange/src/mapi/notifications/hierarchy_table_row_modified_notification_encodes_current_row
  - functions/crates/lpe-exchange/src/mapi/notifications/new_mail_hierarchy_row_notification_encodes_message_row_keys
---

# Signature

`pub(in crate::mapi) fn rop_hierarchy_table_row_modified_response( identity_codec: &crate::mapi::identity::MapiIdentityCodec, notification_handle: u32, logon_id: u8, notification_flags: u16, changed_folder_id: u64, insert_after_folder_id: u64, row_data: &[u8], ) -> Option<Vec<u8>>`

# Calls

- [try_from](../../../../../../functions/crates/lpe-activesync/src/protocol/WbxmlCodePage/tryfrom-u8/try_from.md)
- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [write_u16](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/write_u16.md)
- [append_wire_id](../../../../../../functions/crates/lpe-exchange/src/mapi/notifications/append_wire_id.md)
- [write_u64](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/write_u64.md)

# Called by

- [execute_rops](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute_rops.md)
- [hierarchy_table_row_modified_notification_encodes_current_row](../../../../../../functions/crates/lpe-exchange/src/mapi/notifications/hierarchy_table_row_modified_notification_encodes_current_row.md)
- [new_mail_hierarchy_row_notification_encodes_message_row_keys](../../../../../../functions/crates/lpe-exchange/src/mapi/notifications/new_mail_hierarchy_row_notification_encodes_message_row_keys.md)