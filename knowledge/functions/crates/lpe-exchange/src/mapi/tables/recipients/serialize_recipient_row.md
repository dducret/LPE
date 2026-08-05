---
type: Rust Function
title: serialize_recipient_row
resource: crates/lpe-exchange/src/mapi/tables/recipients.rs#L49-L60
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/write_utf16z
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/rop_read_recipients_response
  - functions/crates/lpe-exchange/src/mapi/rop/responses/rop_open_message_response_with_recipients
  - functions/crates/lpe-exchange/src/mapi/tables/recipients/serialize_pending_recipient_row
---

# Signature

`pub(in crate::mapi) fn serialize_recipient_row(address: &JmapEmailAddress) -> Vec<u8>`

# Calls

- [write_utf16z](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/write_utf16z.md)

# Called by

- [rop_read_recipients_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/rop_read_recipients_response.md)
- [rop_open_message_response_with_recipients](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_open_message_response_with_recipients.md)
- [serialize_pending_recipient_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/recipients/serialize_pending_recipient_row.md)