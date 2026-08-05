---
type: Rust Function
title: serialize_pending_contact_row
resource: crates/lpe-exchange/src/mapi/tables/pending.rs#L494-L521
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/contact/contact_input_from_mapi
  - functions/crates/lpe-exchange/src/mapi/properties/contact/default_contact_for_mapping
  - functions/crates/lpe-exchange/src/mapi/properties/default_mapping_rights
  - functions/crates/lpe-exchange/src/mapi/tables/collaboration_items/serialize_contact_row
  called_by:
  - functions/crates/lpe-exchange/src/mapi/rop/rop_get_properties_specific_response_with_custom
  - functions/crates/lpe-exchange/src/mapi/rop/serialize_object_property
---

# Signature

`pub(in crate::mapi) fn serialize_pending_contact_row( principal: &AccountPrincipal, properties: &HashMap<u32, MapiValue>, columns: &[u32], ) -> Vec<u8>`

# Calls

- [contact_input_from_mapi](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/contact/contact_input_from_mapi.md)
- [default_contact_for_mapping](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/contact/default_contact_for_mapping.md)
- [default_mapping_rights](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/default_mapping_rights.md)
- [serialize_contact_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/collaboration_items/serialize_contact_row.md)

# Called by

- [rop_get_properties_specific_response_with_custom](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/rop_get_properties_specific_response_with_custom.md)
- [serialize_object_property](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/serialize_object_property.md)