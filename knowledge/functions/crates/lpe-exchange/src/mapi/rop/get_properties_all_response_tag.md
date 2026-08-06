---
type: Rust Function
title: get_properties_all_response_tag
resource: crates/lpe-exchange/src/mapi/rop.rs#L1143-L1152
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/tags/MapiPropertyTag/property_type
  called_by:
  - functions/crates/lpe-exchange/src/mapi/rop/rop_get_properties_all_response
---

# Signature

`fn get_properties_all_response_tag(property_tag: u32, want_unicode: bool) -> u32`

# Calls

- [property_type](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/MapiPropertyTag/property_type.md)

# Called by

- [rop_get_properties_all_response](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/rop_get_properties_all_response.md)