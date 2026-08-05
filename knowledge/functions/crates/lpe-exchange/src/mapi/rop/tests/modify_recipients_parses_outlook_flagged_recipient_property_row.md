---
type: Rust Function
title: modify_recipients_parses_outlook_flagged_recipient_property_row
resource: crates/lpe-exchange/src/mapi/rop/tests.rs#L291-L327
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/write_u16
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/write_utf16z
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi/rop/recipients/parse_pending_recipient_row
---

# Signature

`fn modify_recipients_parses_outlook_flagged_recipient_property_row()`

# Calls

- [write_u16](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/write_u16.md)
- [write_utf16z](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/write_utf16z.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [parse_pending_recipient_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/recipients/parse_pending_recipient_row.md)