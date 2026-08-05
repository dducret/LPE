---
type: Rust Function
title: modify_recipients_accepts_microsoft_message_example_columns
resource: crates/lpe-exchange/src/mapi/rop/tests.rs#L1523-L1587
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/write_u16
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi/properties/values/write_ascii_z
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/write_utf16z
  - functions/crates/lpe-exchange/src/mapi/properties/values/write_mapi_value
  - functions/crates/lpe-exchange/src/mapi/rop/recipients/parse_pending_recipient_row
---

# Signature

`fn modify_recipients_accepts_microsoft_message_example_columns()`

# Calls

- [write_u16](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/write_u16.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [write_ascii_z](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/write_ascii_z.md)
- [write_utf16z](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/write_utf16z.md)
- [write_mapi_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/write_mapi_value.md)
- [parse_pending_recipient_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/recipients/parse_pending_recipient_row.md)