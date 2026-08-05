---
type: Rust Function
title: associated_config_modeled_empty_property
resource: crates/lpe-exchange/src/mapi/tables/associated_contents.rs#L460-L482
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/tags/MapiPropertyTag/property_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi/rop/modeled_zero_or_default_property
---

# Signature

`pub(in crate::mapi) fn associated_config_modeled_empty_property( message: Option<&MapiAssociatedConfigMessage>, property_tag: u32, ) -> bool`

# Calls

- [property_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/MapiPropertyTag/property_id.md)

# Called by

- [modeled_zero_or_default_property](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/modeled_zero_or_default_property.md)