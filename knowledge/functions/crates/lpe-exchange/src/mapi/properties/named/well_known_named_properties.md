---
type: Rust Function
title: well_known_named_properties
resource: crates/lpe-exchange/src/mapi/properties/named.rs#L123-L380
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/tags/MapiPropertyTag/property_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/named/well_known_named_property_id
  - functions/crates/lpe-exchange/src/mapi/properties/named/explicit_well_known_named_property_for_id
  - functions/crates/lpe-exchange/src/mapi/properties/tests/well_known_named_property_mappings_are_bijective
---

# Signature

`pub(super) fn well_known_named_properties() -> Vec<(u16, MapiNamedProperty)>`

# Calls

- [property_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/MapiPropertyTag/property_id.md)

# Called by

- [well_known_named_property_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/named/well_known_named_property_id.md)
- [explicit_well_known_named_property_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/named/explicit_well_known_named_property_for_id.md)
- [well_known_named_property_mappings_are_bijective](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/well_known_named_property_mappings_are_bijective.md)