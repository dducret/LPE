---
type: Rust Function
title: attachment_file_extension
resource: crates/lpe-exchange/src/mapi/properties/attachments.rs#L82-L89
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/attachments/attachment_property_value
  - functions/crates/lpe-exchange/src/mapi/tables/attachments/serialize_attachment_row
  - functions/crates/lpe-exchange/src/mapi/tables/attachments/serialize_pending_attachment_row
  - functions/crates/lpe-exchange/src/mapi/tables/attachments/serialize_saved_attachment_row
---

# Signature

`pub(in crate::mapi) fn attachment_file_extension(file_name: &str) -> String`

# Called by

- [attachment_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/attachments/attachment_property_value.md)
- [serialize_attachment_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/attachments/serialize_attachment_row.md)
- [serialize_pending_attachment_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/attachments/serialize_pending_attachment_row.md)
- [serialize_saved_attachment_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/attachments/serialize_saved_attachment_row.md)