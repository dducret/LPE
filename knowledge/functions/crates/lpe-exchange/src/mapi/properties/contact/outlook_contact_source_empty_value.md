---
type: Rust Function
title: outlook_contact_source_empty_value
resource: crates/lpe-exchange/src/mapi/properties/contact.rs#L215-L227
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/tags/MapiPropertyTag/property_type_code
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/contact/contact_property_value_with_identity
---

# Signature

`fn outlook_contact_source_empty_value(property_tag: u32) -> Option<MapiValue>`

# Calls

- [property_type_code](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/MapiPropertyTag/property_type_code.md)

# Called by

- [contact_property_value_with_identity](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/contact/contact_property_value_with_identity.md)