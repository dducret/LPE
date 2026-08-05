---
type: Rust Function
title: well_known_lid_family_property_id
resource: crates/lpe-exchange/src/mapi/properties/named.rs#L68-L87
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/protocol/WbxmlCodePage/tryfrom-u8/try_from
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/named/well_known_named_property_id
---

# Signature

`fn well_known_lid_family_property_id(property: &MapiNamedProperty) -> Option<u16>`

# Calls

- [try_from](../../../../../../../functions/crates/lpe-activesync/src/protocol/WbxmlCodePage/tryfrom-u8/try_from.md)

# Called by

- [well_known_named_property_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/named/well_known_named_property_id.md)