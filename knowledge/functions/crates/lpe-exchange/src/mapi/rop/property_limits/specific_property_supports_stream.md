---
type: Rust Function
title: specific_property_supports_stream
resource: crates/lpe-exchange/src/mapi/rop/property_limits.rs#L129-L134
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/rop/property_limits/size_limited_specific_properties
---

# Signature

`fn specific_property_supports_stream(object: Option<&MapiObject>, tag: u32) -> bool`

# Called by

- [size_limited_specific_properties](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/property_limits/size_limited_specific_properties.md)