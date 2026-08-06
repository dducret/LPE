---
type: Rust Function
title: staged_contact_property_value
resource: crates/lpe-exchange/src/mapi/rop/contact_properties.rs#L64-L81
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/rop/contact_properties/serialize_contact_object_property
---

# Signature

`fn staged_contact_property_value( transaction: &MapiContactTransaction, property_tag: u32, ) -> Option<&MapiValue>`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [serialize_contact_object_property](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/contact_properties/serialize_contact_object_property.md)