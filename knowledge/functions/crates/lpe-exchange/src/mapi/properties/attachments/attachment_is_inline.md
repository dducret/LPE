---
type: Rust Function
title: attachment_is_inline
resource: crates/lpe-exchange/src/mapi/properties/attachments.rs#L41-L47
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/attachments/attachment_property_value
  - functions/crates/lpe-exchange/src/mapi/tables/attachments/serialize_attachment_row
---

# Signature

`pub(in crate::mapi) fn attachment_is_inline(attachment: &MapiAttachment) -> bool`

# Called by

- [attachment_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/attachments/attachment_property_value.md)
- [serialize_attachment_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/attachments/serialize_attachment_row.md)