---
type: Rust Function
title: flagged_property_cell_size
resource: crates/lpe-exchange/src/mapi/rop/property_limits.rs#L111-L127
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/rop/property_limits/size_limited_specific_properties
---

# Signature

`fn flagged_property_cell_size( value_len: usize, typed: bool, unsupported: bool, size_limited: bool, ) -> usize`

# Called by

- [size_limited_specific_properties](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/property_limits/size_limited_specific_properties.md)