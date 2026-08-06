---
type: Rust Function
title: contact_property_value
resource: crates/lpe-exchange/src/mapi/properties/contact.rs#L4-L18
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/contact/contact_property_value_with_identity
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_contact_in_folder
  - functions/crates/lpe-exchange/src/mapi/properties/tests/contact_restriction_uses_projected_folder_context
  - functions/crates/lpe-exchange/src/mapi/tables/collaboration_items/contact_table_property_value
---

# Signature

`pub(in crate::mapi) fn contact_property_value( contact: &AccessibleContact, item_id: u64, folder_id: u64, property_tag: u32, ) -> Option<MapiValue>`

# Calls

- [contact_property_value_with_identity](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/contact/contact_property_value_with_identity.md)

# Called by

- [restriction_matches_contact_in_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_contact_in_folder.md)
- [contact_restriction_uses_projected_folder_context](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/contact_restriction_uses_projected_folder_context.md)
- [contact_table_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/collaboration_items/contact_table_property_value.md)