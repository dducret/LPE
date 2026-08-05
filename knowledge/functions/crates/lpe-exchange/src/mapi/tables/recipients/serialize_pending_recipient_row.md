---
type: Rust Function
title: serialize_pending_recipient_row
resource: crates/lpe-exchange/src/mapi/tables/recipients.rs#L62-L68
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/tables/recipients/serialize_recipient_row
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/rop_read_recipients_response
---

# Signature

`pub(in crate::mapi) fn serialize_pending_recipient_row(recipient: &PendingRecipient) -> Vec<u8>`

# Calls

- [serialize_recipient_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/recipients/serialize_recipient_row.md)

# Called by

- [rop_read_recipients_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/rop_read_recipients_response.md)