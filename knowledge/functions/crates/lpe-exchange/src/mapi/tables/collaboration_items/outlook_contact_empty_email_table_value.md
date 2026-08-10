---
type: Rust Function
title: outlook_contact_empty_email_table_value
resource: crates/lpe-exchange/src/mapi/tables/collaboration_items.rs#L92-L104
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/collaboration_items/serialize_mapi_contact_row
  - functions/crates/lpe-exchange/src/mapi/tables/collaboration_items/contact_table_property_value
---

# Signature

`fn outlook_contact_empty_email_table_value(property_tag: u32) -> Option<MapiValue>`

# Called by

- [serialize_mapi_contact_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/collaboration_items/serialize_mapi_contact_row.md)
- [contact_table_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/collaboration_items/contact_table_property_value.md)