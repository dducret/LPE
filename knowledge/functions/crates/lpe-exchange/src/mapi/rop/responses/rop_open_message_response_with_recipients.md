---
type: Rust Function
title: rop_open_message_response_with_recipients
resource: crates/lpe-exchange/src/mapi/rop/responses.rs#L64-L89
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/tables/recipients/message_recipients
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/write_typed_string
  - functions/crates/lpe-exchange/src/mapi/tables/recipients/serialize_recipient_row
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/message_open/append_open_message_response
---

# Signature

`pub(in crate::mapi) fn rop_open_message_response_with_recipients( request: &RopRequest, subject: &str, email: &JmapEmail, ) -> Vec<u8>`

# Calls

- [message_recipients](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/recipients/message_recipients.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [write_typed_string](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/write_typed_string.md)
- [serialize_recipient_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/recipients/serialize_recipient_row.md)

# Called by

- [append_open_message_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/message_open/append_open_message_response.md)