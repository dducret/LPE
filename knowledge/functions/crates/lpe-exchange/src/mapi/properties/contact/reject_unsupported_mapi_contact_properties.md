---
type: Rust Function
title: reject_unsupported_mapi_contact_properties
resource: crates/lpe-exchange/src/mapi/properties/contact.rs#L633-L678
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/contact/apply_canonical_contact_property_values
---

# Signature

`fn reject_unsupported_mapi_contact_properties(properties: &HashMap<u32, MapiValue>) -> Result<()>`

# Called by

- [apply_canonical_contact_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/contact/apply_canonical_contact_property_values.md)