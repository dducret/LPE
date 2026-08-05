---
type: Rust Function
title: contact_table_property_value
resource: crates/lpe-exchange/src/mapi/tables/collaboration_items.rs#L80-L89
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/contact/contact_property_value
  - functions/crates/lpe-exchange/src/mapi/tables/collaboration_items/outlook_contact_empty_email_table_value
  - functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/collaboration_items/serialize_contact_row
---

# Signature

`pub(super) fn contact_table_property_value( contact: &AccessibleContact, item_id: u64, folder_id: u64, property_tag: u32, ) -> Option<MapiValue>`

# Calls

- [contact_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/contact/contact_property_value.md)
- [outlook_contact_empty_email_table_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/collaboration_items/outlook_contact_empty_email_table_value.md)
- [canonical_property_storage_tag](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag.md)

# Called by

- [serialize_contact_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/collaboration_items/serialize_contact_row.md)