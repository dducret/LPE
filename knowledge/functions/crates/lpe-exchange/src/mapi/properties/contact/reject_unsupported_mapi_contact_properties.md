---
type: Rust Function
title: reject_unsupported_mapi_contact_properties
resource: crates/lpe-exchange/src/mapi/properties/contact.rs#L649-L696
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/contact_transactions/stage_contact_property_values
  - functions/crates/lpe-exchange/src/mapi/properties/contact/apply_canonical_contact_property_values
---

# Signature

`pub(in crate::mapi) fn reject_unsupported_mapi_contact_properties( properties: &HashMap<u32, MapiValue>, ) -> Result<()>`

# Called by

- [stage_contact_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/contact_transactions/stage_contact_property_values.md)
- [apply_canonical_contact_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/contact/apply_canonical_contact_property_values.md)