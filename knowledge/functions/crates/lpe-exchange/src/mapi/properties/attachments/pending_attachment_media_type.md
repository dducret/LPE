---
type: Rust Function
title: pending_attachment_media_type
resource: crates/lpe-exchange/src/mapi/properties/attachments.rs#L130-L135
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/streams/optional_pending_text_property
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/attachments/pending_attachment_upload
  - functions/crates/lpe-exchange/src/mapi/tables/attachments/serialize_pending_attachment_row
---

# Signature

`pub(in crate::mapi) fn pending_attachment_media_type( properties: &HashMap<u32, MapiValue>, ) -> String`

# Calls

- [optional_pending_text_property](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/streams/optional_pending_text_property.md)

# Called by

- [pending_attachment_upload](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/attachments/pending_attachment_upload.md)
- [serialize_pending_attachment_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/attachments/serialize_pending_attachment_row.md)