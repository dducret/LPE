---
type: Rust Function
title: split_object_property_values
resource: crates/lpe-exchange/src/mapi/dispatch/custom_properties.rs#L11-L19
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/split_custom_property_values
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/apply_supported_object_property_values
---

# Signature

`pub(super) fn split_object_property_values( object: &MapiObject, values: Vec<(u32, MapiValue)>, ) -> (Vec<(u32, MapiValue)>, Vec<(u32, MapiValue)>)`

# Calls

- [split_custom_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/split_custom_property_values.md)

# Called by

- [apply_supported_object_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/apply_supported_object_property_values.md)