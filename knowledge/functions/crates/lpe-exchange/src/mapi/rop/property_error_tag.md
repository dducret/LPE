---
type: Rust Function
title: property_error_tag
resource: crates/lpe-exchange/src/mapi/rop.rs#L1136-L1138
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/rop/rop_get_properties_all_response
  - functions/crates/lpe-exchange/src/mapi/rop/tests/get_properties_all_returns_error_tag_for_size_limited_value
---

# Signature

`fn property_error_tag(property_tag: u32) -> u32`

# Called by

- [rop_get_properties_all_response](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/rop_get_properties_all_response.md)
- [get_properties_all_returns_error_tag_for_size_limited_value](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/tests/get_properties_all_returns_error_tag_for_size_limited_value.md)