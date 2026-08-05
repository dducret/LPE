---
type: Rust Function
title: pending_attachment_content_id
resource: crates/lpe-exchange/src/mapi/tables/attachments.rs#L105-L109
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/streams/optional_pending_text_property
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/attachments/serialize_pending_attachment_row
---

# Signature

`fn pending_attachment_content_id(properties: &HashMap<u32, MapiValue>) -> Option<String>`

# Calls

- [optional_pending_text_property](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/streams/optional_pending_text_property.md)

# Called by

- [serialize_pending_attachment_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/attachments/serialize_pending_attachment_row.md)