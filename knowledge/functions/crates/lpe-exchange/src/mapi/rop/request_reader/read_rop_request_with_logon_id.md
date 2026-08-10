---
type: Rust Function
title: read_rop_request_with_logon_id
resource: crates/lpe-exchange/src/mapi/rop/request_reader.rs#L13-L1471
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_u8
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_bytes
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_u16
  - functions/crates/lpe-exchange/src/mapi/rop/parse/parse_tagged_property
  - functions/crates/lpe-exchange/src/mapi/properties/values/write_mapi_value
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_ascii_z
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/remaining
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_i32
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/write_u16_prefixed_bytes
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/write_utf16z
  - functions/crates/lpe-exchange/src/mapi/rop/errors/rop_id_is_reserved
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute/read_next_execute_rop_request
  - functions/crates/lpe-exchange/src/mapi/rop/request_reader/read_rop_request
  - functions/crates/lpe-exchange/src/mapi/session/tests/notification_subscription_preserves_rop_logon_id_through_rop_notify
---

# Signature

`pub(in crate::mapi) fn read_rop_request_with_logon_id( cursor: &mut Cursor<'_>, ) -> Result<(RopRequest, u8)>`

# Calls

- [read_u8](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_u8.md)
- [read_bytes](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_bytes.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [read_u16](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_u16.md)
- [parse_tagged_property](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/parse_tagged_property.md)
- [write_mapi_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/write_mapi_value.md)
- [read_ascii_z](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_ascii_z.md)
- [remaining](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/remaining.md)
- [read_i32](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_i32.md)
- [write_u16_prefixed_bytes](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/write_u16_prefixed_bytes.md)
- [write_utf16z](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/write_utf16z.md)
- [rop_id_is_reserved](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/errors/rop_id_is_reserved.md)

# Called by

- [read_next_execute_rop_request](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute/read_next_execute_rop_request.md)
- [read_rop_request](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/request_reader/read_rop_request.md)
- [notification_subscription_preserves_rop_logon_id_through_rop_notify](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/tests/notification_subscription_preserves_rop_logon_id_through_rop_notify.md)