---
type: Rust Function
title: property_tags_have_known_wire_types
resource: crates/lpe-exchange/src/mapi/dispatch/table_validation.rs#L10-L19
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/tags/MapiPropertyTag/property_type
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute/unknown_property_wire_type_response
---

# Signature

`pub(in crate::mapi::dispatch) fn property_tags_have_known_wire_types( property_tags: &[u32], ) -> bool`

# Calls

- [property_type](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/MapiPropertyTag/property_type.md)

# Called by

- [unknown_property_wire_type_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute/unknown_property_wire_type_response.md)