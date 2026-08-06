---
type: Rust Function
title: contact_object_property_is_deleted
resource: crates/lpe-exchange/src/mapi/rop/contact_properties.rs#L9-L20
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/rop/fallback_default_specific_property
---

# Signature

`pub(in crate::mapi) fn contact_object_property_is_deleted( object: Option<&MapiObject>, property_tag: u32, ) -> bool`

# Called by

- [fallback_default_specific_property](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/fallback_default_specific_property.md)