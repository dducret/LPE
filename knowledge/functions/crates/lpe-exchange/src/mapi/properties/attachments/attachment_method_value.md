---
type: Rust Function
title: attachment_method_value
resource: crates/lpe-exchange/src/mapi/properties/attachments.rs#L63-L69
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/attachments/attachment_is_embedded_message
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/attachments/attachment_property_value
  - functions/crates/lpe-exchange/src/mapi/tables/attachments/serialize_attachment_row
---

# Signature

`pub(in crate::mapi) fn attachment_method_value(attachment: &MapiAttachment) -> u32`

# Calls

- [attachment_is_embedded_message](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/attachments/attachment_is_embedded_message.md)

# Called by

- [attachment_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/attachments/attachment_property_value.md)
- [serialize_attachment_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/attachments/serialize_attachment_row.md)